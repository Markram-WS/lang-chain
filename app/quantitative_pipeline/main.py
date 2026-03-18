import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import numpy as np
import datetime as dt
from model import TradingURL

# Load environment variables from .env file
load_dotenv()


def parse_market_date(date_str):
    current_year = dt.datetime.now().year
    current_date = dt.datetime.now().date()

    # ถ้ามาเป็นรูปแบบเวลา (เช่น 17:02) ให้ใช้ Current Day
    if ":" in str(date_str):
        return current_date

    # ถ้ามาเป็นรูปแบบ Mar/16 ให้เติมปีปัจจุบันเข้าไป
    try:
        # สมมติ Mar/16 คือ Month/Day
        tm = dt.datetime.strptime(f"{date_str}/{current_year}", "%b/%d/%Y")
        return tm.date()
    except:
        return current_date


def get_trading_economics_all_groups(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9,th;q=0.8",
        "Referer": "https://www.google.com/",
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    all_tables = []
    trading = TradingURL()
    for table in soup.find_all("table"):
        table_text = table.get_text()
        if "YTD" in table_text and "Weekly" in table_text:
            df_tmp = pd.read_html(io.StringIO(str(table)))[0]

            for c in df_tmp.columns:
                if "Unnamed" in c:
                    df_tmp = df_tmp.drop(columns=[c])

            group_name = df_tmp.columns[0]

            # Rename คอลัมน์ให้เป็นมาตรฐาน
            # หมายเหตุ: ปรับชื่อให้ตรงกับหน้าเว็บ (บางเว็บใช้ '%' หรือ 'Day%')
            df_tmp = df_tmp.rename(columns=trading.col(url, group_name))

            df_tmp["asset_class"] = group_name

            if "yield" not in df_tmp.columns:
                df_tmp["yield"] = np.nan
            if "marketcap" not in df_tmp.columns:
                df_tmp["marketcap"] = np.nan

            all_tables.append(df_tmp)

            if group_name == "Crypto":
                break

    if all_tables:
        df = pd.concat(all_tables, ignore_index=True)
        df.columns = [c.strip().lower() for c in df.columns]

        df["date"] = df["date"].apply(parse_market_date)

        numeric_cols = [
            "change_val",
            "day_pct",
            "price",
            "weekly",
            "monthly",
            "ytd",
            "yoy",
            "yield",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].replace({np.nan: None})
                if df[col].dtype == "object" or df[col].dtype == "str":
                    df[col] = df[col].str.replace(",", "", regex=False)
                    df[col] = (
                        df[col].str.replace("%", "", regex=False).astype(float) / 100
                    )
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        # if 'marketcap' in df.columns:
        #     df['marketcap'] = df['marketcap'].str.replace('$', '', regex=False)
        #     df['marketcap'] = df['marketcap'].str.replace(',', '', regex=False)

        return df
    else:
        return None


def db_conn():
    user = os.getenv("POSTGRES_USER", "user")
    password = os.getenv("POSTGRES_PASSWORD", "pass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "market_db")

    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")


def setup(engine):
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS market_stats (
                asset_class TEXT,
                yield FLOAT,
                change_val FLOAT,
                day_pct FLOAT,
                price FLOAT,
                weekly FLOAT,
                monthly FLOAT,
                marketcap TEXT,
                ytd FLOAT,
                yoy FLOAT,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        )

        conn.execute(
            text("""
            truncate table market_stats;
        """)
        )


def upsert(df, engine):
    if len(df):
        # --- 4. Insert using UPSERT (SQLAlchemy) ---
        with engine.begin() as conn:
            df.to_sql(name="market_stats", con=engine, if_exists="append", index=False)
            print(f"Successfully upserted {len(df)} records to Postgres.")
    else:
        print("SKIP")


if __name__ == "__main__":
    groups = [
        "https://tradingeconomics.com/commodities",
        "https://tradingeconomics.com/stocks",
        "https://tradingeconomics.com/currencies",
        "https://tradingeconomics.com/crypto",
        "https://tradingeconomics.com/bonds",
    ]

    for g in groups[:]:
        print(f"process : {g}")
        data = get_trading_economics_all_groups(g)
        if len(data):
            eng = db_conn()
            setup(eng)
            upsert(data, eng)
        else:
            print("get_trading_economics Null")
