# BotDichVuSopee

Bot Telegram backup, tách riêng khỏi bot cũ để chạy an toàn.

## Chức năng nút bấm
- `Q Check`
- `Mua ACC Shopee`
- `Mua Số Shopee`
- `Lọc Số Shopee`
- `Add Voucher`
- `Lấy Cookie QR`
- `Add Mail`
- `ADD Địa Chỉ - Sản Phẩm`
- `Làm mới F -> ST`

Luồng mặc định: bấm nút -> bot yêu cầu gửi cookie -> bot gọi API backend theo tính năng.

## Chạy local
```powershell
cd BotDichVuSopee
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python bot_dich_vu_shopee.py
```

## Deploy Railway
- Root Directory: `BotDichVuSopee`
- Start command lấy từ `Procfile`
- Set biến môi trường theo `.env.example`

## Lưu ý backend
Bot này là lớp giao tiếp Telegram. Mỗi tính năng gọi endpoint backend tương ứng:
- `EP_CHECK`
- `EP_BUY_ACC`
- `EP_BUY_PHONE`
- `EP_FILTER_PHONE`
- `EP_ADD_VOUCHER`
- `EP_COOKIE_QR`
- `EP_ADD_MAIL`
- `EP_ADD_ADDRESS`
- `EP_REFRESH_F_ST`

Có thể set endpoint theo relative path (kèm `API_BASE_URL`) hoặc full URL.
