# Git Push Context (BotaddvcRailway)

Cap nhat: 2026-03-23

## Muc dich
Luu cau hinh git de lan sau co the push len GitHub nhanh, dung repo.

## Thong tin repo
- Local path: `e:\TOOLMMO\BotADD-RailWay`
- GitHub repo: `https://github.com/Shirovn55/BotaddvcRailway.git`
- Branch mac dinh: `main`
- Chu so huu: `Shirovn55`
- Ten repo: `BotaddvcRailway`

## Trang thai hien tai
- Thu muc hien tai CHUA co `.git`.

## Lenh push chuan (chay trong `e:\TOOLMMO\BotADD-RailWay`)
```powershell
git init -b main
git remote add origin https://github.com/Shirovn55/BotaddvcRailway.git
git add -A
git commit -m "chore: update bot anti-spam hardening"
git push -u origin main
```

## Neu da co `.git` roi
```powershell
git remote set-url origin https://github.com/Shirovn55/BotaddvcRailway.git
git checkout main
git add -A
git commit -m "<commit message>"
git push origin main
```

## Ghi chu auth
- Neu push bi hoi dang nhap: dung GitHub PAT hoac Git Credential Manager tren may cua ban.
- Khong luu token vao file nay.

