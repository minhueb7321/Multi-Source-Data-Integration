import os; import pandas as pd
from sqlalchemy import create_engine, Date
from dotenv import load_dotenv
from urllib.parse import quote_plus
from unidecode import unidecode

load_dotenv()

# Khởi tạo Engine một lần duy nhất
# Sử dụng pool_size để quản lý kết nối hiệu quả hơn
CONNECTION_STR = f"postgresql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASS'))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(CONNECTION_STR, pool_size=10, max_overflow=20) # ở đây tạo sẵn 10 cổng kết nối, max là 20 


def decode_name_df(df:pd.DataFrame) -> pd.DataFrame:

    """chuyen thanh snake_case cho cac bang upload vao"""

    df.columns = [unidecode(c).lower().replace(" ", "_") for c in df.columns]

    return df


# Phiên bản tối ưu để đẩy dữ liệu vào Postgres
def load_to_postgres_optimized(df, table_name, schema_name='stg', if_exists='replace'):
    """
    Phiên bản tối ưu để đẩy dữ liệu vào Postgres
    """
    if df.empty:
        print("⚠️ DataFrame rỗng, không có gì để đẩy.")
        return

    # Chuẩn hóa tên cột
    df = decode_name_df(df)
    
    try:
        df.to_sql(
            name=table_name, 
            con=engine, 
            if_exists=if_exists, 
            schema=schema_name, 
            index=False,
            dtype={'ngay_hach_toan': Date},
            chunksize=5000, 
            method='multi' 
        )
        print(f"✅ Đẩy vào Postgres thành công: {schema_name}.{table_name} ({len(df)} dòng)")
    except Exception as e:
        print(f"❌ Lỗi khi đẩy dữ liệu: {e}")
