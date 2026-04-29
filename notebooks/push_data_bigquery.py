from google.cloud import bigquery
import pandas as pd
import os


df_2025 = pd.read_excel(io= r'')
# 1. Khởi tạo Client (đảm bảo đã trỏ tới file JSON key)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r''

client = bigquery.Client()

# 2. Giả sử bạn có DataFrame df
# df = pd.read_csv(...)

table_id = "mydicosilver.sct_data.sct2025"

# 3. Cấu hình LoadJobConfig
job_config = bigquery.LoadJobConfig(
    # Lựa chọn cách ghi:
    # WRITE_TRUNCATE: Xóa bảng cũ ghi đè bảng mới
    # WRITE_APPEND: Ghi nối tiếp vào bảng đã có
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,

    # Tự động nhận diện Schema (cột nào là chuỗi, cột nào là số)
    autodetect=True,

    # Nếu bạn muốn cố định Schema (không cho BQ tự đoán) thì dùng dòng dưới:
    # schema=[
    #     bigquery.SchemaField("col_name_1", "STRING"),
    #     bigquery.SchemaField("col_name_2", "FLOAT"),
    # ],
)

# 4. Thực thi Job
job = client.load_table_from_dataframe(df_2025, table_id, job_config=job_config)

# Đợi job hoàn thành
job.result()

print(f"Đã đẩy thành công {job.output_rows} dòng vào bảng {table_id}")

# 99293 
