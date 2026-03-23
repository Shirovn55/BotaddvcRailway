# -*- coding: utf-8 -*-
"""
import_wallet_to_postgres.py
Import tab "ThanhToan"/"Thanh Toan" (wallet) từ CSV/Excel vào PostgreSQL (Railway).

✅ Tự động chuẩn hoá tên cột (tele_id / balance / username / status / notes / gift / pass)
✅ Xoá trùng trong file import: giữ lại dòng có "balance" cao nhất theo tele_id
✅ Upsert lên DB: nếu tele_id đã tồn tại thì CHỈ update khi balance mới >= balance đang có
   (đảm bảo "giữ lại data tiền cao nhất" ngay cả khi DB đã có dữ liệu cũ)

Usage:
  pip install pandas psycopg2-binary openpyxl

  # import từ CSV (khuyến nghị)
  python import_wallet_to_postgres.py --csv "Shopee - Thanh Toan.csv"

  # import từ Excel (sheet có thể là "ThanhToan" hoặc "Thanh Toan")
  python import_wallet_to_postgres.py --xlsx Shopee.xlsx --sheet "ThanhToan"
"""

import os, sys, argparse
import pandas as pd
import psycopg2

# ---------- Helpers ----------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace("\ufeff", "", regex=False)  # BOM
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df

def _pick_first_existing(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def load_df(args) -> pd.DataFrame:
    if args.csv:
        # sep=None + engine="python" để tự nhận dạng , ; \t
        df = pd.read_csv(args.csv, encoding="utf-8-sig", sep=None, engine="python")
    else:
        xlsx = args.xlsx
        sheet = args.sheet
        try_sheets = [sheet]
        # fallback sheet name
        if sheet.lower().replace(" ", "") == "thanhtoan":
            try_sheets += ["Thanh Toan", "ThanhToan"]
        else:
            try_sheets += ["ThanhToan", "Thanh Toan"]

        last_err = None
        for sh in try_sheets:
            try:
                df = pd.read_excel(xlsx, sheet_name=sh)
                break
            except Exception as e:
                last_err = e
                df = None
        if df is None:
            raise last_err

    df = _normalize_columns(df)

    col_tele = _pick_first_existing(df, ["tele_id", "teleid", "telegram_id", "id_tele", "tele"])
    if not col_tele:
        raise SystemExit("❌ Không tìm thấy cột tele_id (tele_id/teleid/telegram_id/id_tele/tele) trong file import")

    col_user = _pick_first_existing(df, ["username", "user", "name", "ten", "fullname"])
    col_bal  = _pick_first_existing(df, ["balance", "money", "so_du", "sodu", "tien", "amount"])
    col_sta  = _pick_first_existing(df, ["status", "trang_thai"])
    col_note = _pick_first_existing(df, ["notes", "note", "ghi_chu", "ghichu"])
    col_gift = _pick_first_existing(df, ["gift", "qua", "quatang"])
    col_pass = _pick_first_existing(df, ["pass", "password", "tool_pass", "toolpass"])

    out = pd.DataFrame()
    out["tele_id"] = pd.to_numeric(df[col_tele], errors="coerce")
    out = out[out["tele_id"].notna()].copy()
    out["tele_id"] = out["tele_id"].astype("int64")

    out["username"] = (df[col_user] if col_user else "").fillna("").astype(str)
    out["balance"] = pd.to_numeric(df[col_bal] if col_bal else 0, errors="coerce").fillna(0).round(0).astype("int64")
    out["status"] = (df[col_sta] if col_sta else "active").fillna("active").astype(str)
    out["notes"] = (df[col_note] if col_note else "").fillna("").astype(str)
    out["gift"] = (df[col_gift] if col_gift else "").fillna("").astype(str)
    out["tool_pass"] = (df[col_pass] if col_pass else "").fillna("").astype(str)

    # ---- Deduplicate in import file: keep highest balance per tele_id ----
    before = len(out)
    out = out.sort_values(["tele_id", "balance"], ascending=[True, False])
    out = out.drop_duplicates(subset=["tele_id"], keep="first")
    removed = before - len(out)
    if removed:
        print(f"🧹 Removed {removed} duplicated rows in import file (kept highest balance per tele_id)")

    return out[["tele_id", "username", "balance", "status", "notes", "gift", "tool_pass"]]

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="CSV export (vd: Shopee - Thanh Toan.csv)")
    ap.add_argument("--xlsx", help="Excel file (.xlsx)")
    ap.add_argument("--sheet", default="ThanhToan", help='Tên sheet trong Excel (vd: "ThanhToan" hoặc "Thanh Toan")')
    args = ap.parse_args()

    if not args.csv and not args.xlsx:
        ap.error("Bạn phải nhập --csv hoặc --xlsx")

    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn:
        print("❌ Thiếu DATABASE_URL env")
        sys.exit(1)

    df = load_df(args)
    print(f"✅ Loaded {len(df)} wallet rows for import")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet (
        tele_id BIGINT PRIMARY KEY,
        username TEXT,
        balance BIGINT NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'active',
        notes TEXT,
        gift TEXT,
        tool_pass TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
    conn.commit()

    # Giữ lại balance cao nhất: chỉ update khi balance mới >= balance hiện tại
    sql = """
    INSERT INTO wallet (tele_id, username, balance, status, notes, gift, tool_pass, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s, NOW())
    ON CONFLICT (tele_id) DO UPDATE
    SET username=EXCLUDED.username,
        balance=EXCLUDED.balance,
        status=EXCLUDED.status,
        notes=EXCLUDED.notes,
        gift=EXCLUDED.gift,
        tool_pass=EXCLUDED.tool_pass,
        updated_at=NOW()
    WHERE EXCLUDED.balance >= wallet.balance;
    """

    ok = 0
    for row in df.itertuples(index=False):
        cur.execute(sql, (int(row.tele_id), row.username, int(row.balance), row.status, row.notes, row.gift, row.tool_pass))
        ok += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Processed {ok} rows (upsert giữ balance cao nhất)")

if __name__ == "__main__":
    main()
