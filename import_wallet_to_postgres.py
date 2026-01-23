# -*- coding: utf-8 -*-
"""
import_wallet_to_postgres.py
Import tab "Thanh Toan" (wallet) từ Excel/CSV vào PostgreSQL (Railway)

Usage:
  pip install pandas psycopg2-binary openpyxl

  # import từ CSV
  python import_wallet_to_postgres.py --csv wallet_import.csv

  # import trực tiếp từ Excel
  python import_wallet_to_postgres.py --xlsx Shopee.xlsx --sheet "Thanh Toan"
"""

import os, sys, argparse
import pandas as pd
import psycopg2

def load_df(args):
    if args.csv:
        df = pd.read_csv(args.csv)
    else:
        df = pd.read_excel(args.xlsx, sheet_name=args.sheet)

    df["tele_id"] = pd.to_numeric(df.get("tele_id"), errors="coerce")
    df = df[df["tele_id"].notna()].copy()
    df["tele_id"] = df["tele_id"].astype("int64")

    df["username"] = df.get("username", "").fillna("").astype(str)
    df["balance"] = pd.to_numeric(df.get("balance", 0), errors="coerce").fillna(0).astype(float).round(0).astype("int64")
    df["status"] = df.get("status", "active").fillna("active").astype(str)
    df["notes"] = df.get("notes", "").fillna("").astype(str)
    df["gift"] = df.get("gift", "").fillna("").astype(str)

    return df[["tele_id", "username", "balance", "status", "notes", "gift"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="CSV wallet export")
    ap.add_argument("--xlsx", help="Excel file (.xlsx)")
    ap.add_argument("--sheet", default="Thanh Toan", help="Tên sheet trong Excel")
    args = ap.parse_args()

    if not args.csv and not args.xlsx:
        ap.error("Bạn phải nhập --csv hoặc --xlsx")

    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn:
        print("❌ Thiếu DATABASE_URL env")
        sys.exit(1)

    df = load_df(args)
    print(f"✅ Loaded {len(df)} wallet rows")

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
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
    conn.commit()

    sql = """
    INSERT INTO wallet (tele_id, username, balance, status, notes, gift, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s, NOW())
    ON CONFLICT (tele_id) DO UPDATE
    SET username=EXCLUDED.username,
        balance=EXCLUDED.balance,
        status=EXCLUDED.status,
        notes=EXCLUDED.notes,
        gift=EXCLUDED.gift,
        updated_at=NOW();
    """

    ok = 0
    for row in df.itertuples(index=False):
        cur.execute(sql, (int(row.tele_id), row.username, int(row.balance), row.status, row.notes, row.gift))
        ok += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Imported/Upserted {ok} rows into wallet")

if __name__ == "__main__":
    main()
