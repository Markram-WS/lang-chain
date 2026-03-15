import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
from sqlalchemy import create_engine, text


def get_trading_economics_all_groups(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        all_tables = []

        # วนลูปหาทุกตารางในหน้า
        for table in soup.find_all("table"):
            # เช็คว่าตารางนี้มีข้อมูลที่เราต้องการ (มีคอลัมน์ Price และ Weekly)
            table_text = table.get_text()
            if "Price" in table_text and "Weekly" in table_text:
                # ใช้ io.StringIO ป้องกัน OSError
                df_tmp = pd.read_html(io.StringIO(str(table)))[0]

                # --- ปรับ Pattern ชื่อคอลัมน์ ---
                # เนื่องจากคอลัมน์แรกสุดคือชื่อกลุ่ม (xxxxx) เราจะเปลี่ยนเป็น 'Symbol/Name'
                # และเก็บชื่อกลุ่มดั้งเดิมไว้ในคอลัมน์ใหม่ชื่อ 'Group'
                group_name = df_tmp.columns[0]
                df_tmp = df_tmp.rename(columns={group_name: "Name"})
                df_tmp["Group"] = group_name

                all_tables.append(df_tmp)

        if not all_tables:
            return None

        # รวมทุกกลุ่มเข้าด้วยกัน (Energy + Metals + Agri + ...)
        df_final = pd.concat(all_tables, ignore_index=True)

        # Clean ชื่อคอลัมน์ให้ไม่มีช่องว่าง
        df_final.columns = [c.strip() for c in df_final.columns]

        # กำจัดแถวที่เป็น Header ซ้ำ (กรณี scrap ติดมา)
        df_final = df_final[df_final["Price"] != "Price"]

        return df_final

    except Exception as e:
        print(f"Error: {e}")
        return None


def clean_and_upsert_commodities(df):
    if df is None or df.empty:
        print("No data to process")
        return

    # --- 1. Data Cleaning ---
    # คอลัมน์ที่ต้องการแปลงเป็นตัวเลข
    numeric_cols = ["Price", "Day", "Weekly", "Monthly", "YTD", "YoY"]

    for col in numeric_cols:
        if col in df.columns:
            # ลบ %, +, , (คอมม่า) และช่องว่างออก
            df[col] = df[col].astype(str).str.replace(r"[%+,\s]", "", regex=True)
            # แปลงเป็นตัวเลข ถ้าแปลงไม่ได้ให้เป็น NaN
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ลบแถวที่ไม่มีชื่อ Name หรือไม่มีราคา (ข้อมูลขยะ)
    df = df.dropna(subset=["Name", "Price"])

    # --- 2. Database Connection (M700 Docker) ---
    # ปรับแต่ง URL ตามที่คุณตั้งค่าไว้ใน Docker Compose
    # รูปแบบ: postgresql://username:password@localhost:5432/dbname
    engine = create_engine("postgresql://user:pass@localhost:5432/market_db")

    # --- 3. Insert using UPSERT (SQLAlchemy) ---
    # เราจะใช้คำสั่ง SQL เพื่อจัดการกรณีที่ Symbol/Name ซ้ำให้ทำการ Update แทน
    with engine.begin() as conn:
        for _, row in df.iterrows():
            insert_query = text("""
                INSERT INTO market_stats (group_name, name, price, weekly_chg, monthly_chg, yoy_chg, last_updated)
                VALUES (:group, :name, :price, :weekly, :monthly, :yoy, CURRENT_TIMESTAMP)
                ON CONFLICT (name) 
                DO UPDATE SET 
                    price = EXCLUDED.price,
                    weekly_chg = EXCLUDED.weekly_chg,
                    monthly_chg = EXCLUDED.monthly_chg,
                    yoy_chg = EXCLUDED.yoy_chg,
                    last_updated = EXCLUDED.last_updated;
            """)

            conn.execute(
                insert_query,
                {
                    "group": row["Group"],
                    "name": row["Name"],
                    "price": row["Price"],
                    "weekly": row["Weekly"],
                    "monthly": row["Monthly"],
                    "yoy": row["YoY"],
                },
            )

    print(f"Successfully upserted {len(df)} records to Postgres.")


if __name__ == "__main__":
    groups = [
        "https://tradingeconomics.com/commodities",
        "https://tradingeconomics.com/stocks",
        "https://tradingeconomics.com/currencies",
        "https://tradingeconomics.com/crypto",
        "https://tradingeconomics.com/bonds",
    ]
    for g in groups:
        print(f"process : {g}")
        data = get_trading_economics_all_groups(g)
        clean_and_upsert_commodities(data)
        print("\ndone \n")
