# Beyond MMR – คู่มือใช้งานเร็ว (ภาษาไทย)

คำสั่งที่ใช้บ่อย (ก๊อปแล้วรันได้ทันที):
1. ติดตั้งไลบรารี: `python -m pip install -r requirements.txt`
2. เปิด Postgres (Docker): `docker compose up -d`
3. รันทั้ง pipeline (ingest → transform → publish): `python -u run_pipeline.py`
4. รันเฉพาะ publish (หลังจากมีตาราง production แล้ว): `python -u publish.py`
5. ปิด Docker: `docker compose down`

## ภาพรวมโปรเจกต์
- ไหลงาน: CSV → Postgres (raw_data) → Postgres (production, ทำความสะอาด/เพิ่มฟีเจอร์) → Google Sheets → Looker Studio
- โฟกัส: เปรียบเทียบ `sellingprice` vs `mmr`, เพิ่มฟีเจอร์ price diff/percent และคัดข้อมูลให้พร้อมทำ Dashboard

## การตั้งค่า
1) ติดตั้ง Python deps  
   `python -m pip install -r requirements.txt`

2) ตั้งค่า .env  
   คัดลอก `.env.example` เป็น `.env` แล้วกรอกค่า:
   - DB: `DB_USER=car_user`, `DB_PASSWORD=car_password`, `DB_NAME=car_db`, `DB_HOST=localhost`, `DB_PORT=5432`
   - Google Sheets: `GOOGLE_SHEETS_SPREADSHEET_ID=<ใส่ไอดีชีตของคุณ>`
   - ไฟล์ credential: `GOOGLE_APPLICATION_CREDENTIALS=./service_account.json` (หรือใช้ `GOOGLE_CREDENTIALS_JSON`)

3) เริ่ม Postgres ด้วย Docker  
   `docker compose up -d`  
   เช็กสถานะ: `docker compose ps`

## การรัน pipeline
- รันครบทุกขั้นตอน (แนะนำ):  
  `python -u run_pipeline.py`  
  ขั้นตอน: ingest CSV → raw_data | transform สร้าง production tables | publish ขึ้น Google Sheets (เช็กลิมิตเซลล์ให้)

- รันเฉพาะ publish (ใช้ตาราง production ที่มีอยู่แล้ว):  
  `python -u publish.py`

- ปิดบริการ:  
  `docker compose down`

## กติกาการทำความสะอาดข้อมูล (transform)
- ตัดแถวที่ขาดฟิลด์หลัก: vin, year, state, saledate, sellingprice, mmr, odometer, condition
- ช่วงค่าที่รับได้: sellingprice > 0; mmr > 0; year 1950–(ปีปัจจุบัน+1); odometer 0–1,500,000; condition 0–5
- วันที่: แปลง saledate เป็น UTC; ต้องมี sale_datetime; vehicle_age > 0 เท่านั้น
- การจัดรูปแบบสตริง: trim; state เป็นตัวพิมพ์ใหญ่; make/model/trim/body/transmission/color/interior/seller เป็น Title Case; vin เป็นตัวพิมพ์ใหญ่
- ลบซ้ำตามคู่ (vin, sale_datetime) เก็บแถวแรก
- ฟีเจอร์ที่สร้าง: sale_year/month/quarter, vehicle_age, price_diff, price_diff_pct, is_above_mmr, odometer_per_year

## พฤติกรรมตอนส่งขึ้น Google Sheets
- เคารพลิมิต 10M เซลล์: `vehicle_sales_enriched` ส่งเฉพาะคอลัมน์สำคัญและตั้งเพดาน ~8M เซลล์ (ตอนนี้หลังทำความสะอาดได้ ~64k แถว)
- Summary แท็บเล็ก ส่งครบ: `sales_summary_by_make_month`, `sales_summary_by_state_month`
- วันที่ใน Sheets เป็นรูป `YYYY-MM-DD` เพื่อ Group ต่อใน Looker Studio ได้ง่าย

## รายการไฟล์
- `ingest.py` – โหลด CSV → `raw_data.vehicle_sales`
- `transform.py` – ทำความสะอาด + สร้างฟีเจอร์ → ตารางใน `production`
- `publish.py` – ส่งตาราง production ไป Google Sheets
- `run_pipeline.py` – รันครบ ingest → transform → publish
- `docker-compose.yml` – คอนฟิก Postgres
- `.env.example` – ตัวอย่างไฟล์ตั้งค่า.

## แก้ปัญหาเบื้องต้น
- Docker ไม่รัน: เปิด Docker Desktop แล้วสั่ง `docker compose up -d`
- ปัญหา credential: ตรวจ path `service_account.json` ใน `.env` หรือใช้ `GOOGLE_CREDENTIALS_JSON`
- ชนลิมิตเซลล์: ลดคอลัมน์/จำนวนแถว หรือแบ่งหลายชีต ปรับค่า `SHEET_CELL_LIMITS` ใน `publish.py` (รวมทั้งไฟล์ต้องต่ำกว่า 10M เซลล์)
