# folder others -> customers : file Duc, products : file Duc
# folder others -> contracts : file hop dong
# folder others -> employees : file HRM cua HR


# misa -> customers : misa -> all_customers_misa.xlsx & stg.misa_customers (Postgre)
# misa-> employees : misa -> all_employees_misa.xlsx & stg.misa_employees
# misa-> products : misa -> all_products_misa.xlsx & stg.misa_products

# folder dms -> customers : 4 files KH, TN -> stg.dms_dskh, stg.dms_dskh_chung, stg.dms_dstn, stg.dms_dstn_chung
# folder dms -> employees -> stg.dms_employees
# folder dms -> products -> stg.dms_products

# misa_products + dms_products + duc_products => int.products
# misa_employees + dms_employees + hrm file => int.employees
# misa_customers + dms_customers + hrm_customers => int.customers

# file hợp đồng => stg.contracts
# file kpi từng chi nhánh => stg.kpi_branches


# Module liên quan tới database
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Module xử lý dữ liệu
import re ; import os
import pandas as pd

# Module xử lý trên MISA
from scr.transform_misa_employees import concatnate_all_employees
from scr.transform_misa_customers import concatenate_all_customers

# Module xử lý trên DMS
from scr.transform_dms_customers import lightning_transform

# Module de day vao database Postgre
from scr.db_utils import load_to_postgres_optimized

# Module để chạy đa luồng
# from concurrent.futures import ThreadPoolExecutor


# Hàm để thực thi file trong folder ETL\postgres
def execute_sql_file(file_path, engine):
    """Đọc và thực thi toàn bộ nội dung trong file .sql"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_script = file.read()
        
        # Thực thi script
        with engine.connect() as connection:
            # Dùng text() để đảm bảo chuỗi được hiểu là lệnh SQL
            from sqlalchemy import text
            connection.execute(text(sql_script))
            # Đối với một số phiên bản SQLAlchemy, cần commit nếu không dùng context manager tự động
            connection.commit()
            
        print(f"✔️ Đã thực thi thành công file: {file_path}")
    except Exception as e:
        print(f"❌ Lỗi khi thực thi file SQL: {e}")



load_dotenv() # Nạp biến từ .env
# Hàm tạo engine để kết nối
def get_engine():
    # Chuỗi kết nối Postgres: postgresql://user:password@host:port/dbname
    connection_str = f"postgresql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASS'))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(connection_str, pool_size=10, max_overflow=20)



# Hàm xử lý các tệp trên DMS (tần suất update: nửa tháng 1 lần)
def task_process_dms(dms_dir, engine):
    # Xử lý file khách hàng trên DMS
    results_process_dms_customers = lightning_transform(
        file_name_kh = os.path.join(dms_dir, 'customers','Danh sách khách hàng.xlsx'),
        file_name_kh_chung= os.path.join(dms_dir, 'customers','Danh sách khách hàng chung.xlsx'),
        file_name_tn= os.path.join(dms_dir, 'customers','Danh sách tiềm năng.xlsx'),
        file_name_tn_chung= os.path.join(dms_dir, 'customers','Danh sách tiềm năng chung.xlsx')
        ) # Hàm tính toán ra file khách hàng tổng duy nhất
    
    names = ['dms_dskh', 'dms_dstn' , 'dms_dskh_chung', 'dms_dstn_chung']
    for df_process_customer, name in zip(results_process_dms_customers, names): # nhét vào zip tuần tự
        load_to_postgres_optimized(df = df_process_customer, table_name= name, schema_name='stg')

    # Xử lý file nhân sự trên DMS
    from scr.transform_dms_employees import lightning_transform_employees
    df_employees = lightning_transform_employees(os.path.join(dms_dir, 'employees','Danh sách nhân viên.xlsx'))
    load_to_postgres_optimized(df_employees, table_name = 'dms_employees', schema_name='stg')

    # Xử lý file sản phẩm trên DMS
    from scr.transform_dms_products import light_trasform_dms_products
    df_dms_products = light_trasform_dms_products(os.path.join(dms_dir, 'products','Danh sách sản phẩm.xlsx'))
    load_to_postgres_optimized(df_dms_products, table_name = 'dms_products', schema_name='stg')


# Hàm xử lý các tệp trên Misa (tần suất update : mỗi lần chuyển dữ liệu sang Misa mới)
def task_process_misa(base_dir, engine):
    #  Xử lý file nhân sự trên misa
    RAW_EMPLOYEE_FOLDER = os.path.join(base_dir, 'data', 'raw', 'misa', 'employees')
    OUTPUT_EMPLOYEE_FOLDER = os.path.join(base_dir, 'data', 'processed', 'misa', 'employees')
    df_all_employees = concatnate_all_employees(RAW_EMPLOYEE_FOLDER)
    df_all_employees_to_postgres = df_all_employees.copy()
    load_to_postgres_optimized(df_all_employees_to_postgres, table_name='misa_employees', schema_name='stg')

    # Nhân viên Quyền có 2 mã nối vào df chính sẽ bị dup doanh số nên xóa bỏ mã bên VH
    df_all_employees = df_all_employees[df_all_employees['Mã nhân viên'] != '']
    file_name_individual_employees = "all_employees_misa.xlsx"
    output_path_employees = os.path.join(OUTPUT_EMPLOYEE_FOLDER, file_name_individual_employees)
    df_all_employees.to_excel(output_path_employees, index=False)
    
    # Xử lý file khách hàng trên misa
    RAW_CUSTOMERS_FOLDER = os.path.join(base_dir, 'data', 'raw', 'misa', 'customers')
    OUTPUT_CUSTOMERS_FOLDER = os.path.join(base_dir, 'data', 'processed', 'misa', 'customers')
     # up vao Postgres
    df_all_customers = concatenate_all_customers(RAW_CUSTOMERS_FOLDER)
    df_all_customers.to_excel(os.path.join(OUTPUT_CUSTOMERS_FOLDER, "all_customers_misa.xlsx"), index=False)
    load_to_postgres_optimized(df_all_customers, table_name='misa_customers', schema_name='stg')
    
    # Xử lý file sản phẩm trên misa
    RAW_PRODUCTS_FOLDER = os.path.join(base_dir, 'data', 'raw', 'misa', 'products')
    OUTPUT_PRODUCTS_FOLDER = os.path.join(base_dir, 'data', 'processed', 'misa', 'products')
    from scr.transform_misa_products import concatenate_all_products  
    df_all_products = concatenate_all_products(RAW_PRODUCTS_FOLDER)
    df_all_products.to_excel(os.path.join(OUTPUT_PRODUCTS_FOLDER, "all_products_misa.xlsx"), index=False)
    load_to_postgres_optimized(df_all_products, table_name='misa_products', schema_name='stg')  


# Hàm xử lý các file ngoài (tần suất: phụ thuộc vào từng mục)
def task_process_others(base_dir, engine):
    # Xử lý file HRM trên file của nhân sự (Tần suất update : 1 lần/tuần)
    RAW_OTHERS_HRM = os.path.join(base_dir, 'data', 'raw', 'others', 'employees', 'Danh sách nhân viên MYDICO.xlsx')
    # OUTPUT_OTHERS_FOLDER = os.path.join(BASE_DIR, 'data', 'processed', 'others', 'employees')
    df_others_hrm = pd.read_excel(RAW_OTHERS_HRM, sheet_name= 'DATA TỔNG')
    df_others_hrm = df_others_hrm.iloc[:, :-2]
    load_to_postgres_optimized(df_others_hrm, table_name= 'hrm_employees', schema_name='stg')

    # Xử lý file customers của Đức (Tần suất : Đức đã nghỉ, để nguyên không quan tâm)
    RAW_OTHERS_CUSTOMERS = os.path.join(base_dir, 'data', 'raw', 'others', 'customers', 'Danh sách khách hàng.xlsx')
    df_others_customers = pd.read_excel(RAW_OTHERS_CUSTOMERS)
    load_to_postgres_optimized(df_others_customers, table_name = 'duc_dskh', schema_name='stg')

    # Xử lý file products của Đức (Tần suất : Đức đã nghỉ, để nguyên không quan tâm)
    RAW_OTHERS_PRODUCTS = os.path.join(base_dir, 'data', 'raw', 'others', 'products', 'Danh sách sản phẩm.xlsx')
    df_others_products = pd.read_excel(RAW_OTHERS_PRODUCTS)
    load_to_postgres_optimized(df_others_products, table_name = 'duc_dssp', schema_name='stg')


    # Xử lý file hợp đồng trên Sheets (Tần suất : 1 tuần 1 lần hoặc có request gửi báo cáo từ các shakholder)
    path_hop_dong = os.path.join(BASE_DIR, 'data', 'raw', 'others' ,'contracts', 'TỔNG Mydico_Bảng theo dõi hợp đồng.xlsx')
    df_hop_dong = pd.read_excel(path_hop_dong, sheet_name='Data tổng cty')
    load_to_postgres_optimized(df_hop_dong,table_name='contracts', schema_name='stg')
    

    # Xử lý file KPI của a Thuấn (Tần suất : 1 lần/năm)
    # Cần nhập lại KPI tại đây để tính toán
    set_year_kpi = 2026
    # Nhớ thay đổi tên file
    path_KPI = os.path.join(BASE_DIR, 'data', 'raw', 'others', 'kpi_branches', 'Doanh số năm 2026.xlsx' ) 

    # Hàm xử lý data sale forecast trong file
    def transform_kpi_data(path):
        df_kpi = pd.read_excel(path, sheet_name = 'DS toàn công ty')
        df_kpi_new = df_kpi.melt(id_vars=['Phòng ban'],
                            var_name = 'Month_Raw',
                            value_name='Target'
                            )
        df_kpi_new = df_kpi_new[df_kpi_new['Phòng ban'] != 'Chỉ tiêu 2026']

        # df_kpi_new['Target'] = df_kpi_new['Target'].replace('.', '').astype('Int64')
        def transform_year_key(row, base_year):
                raw = str(row['Month_Raw'])
                if raw == 'Tổng':
                    return 'Tổng'
                if ',' in raw :
                    match = re.search(r'Tháng\s*(\d+),\s*(\d+)', raw)
                    if match:
                        month_num = int(match.group(1))
                        actual_year = int(match.group(2))
                        return f"{actual_year}-{month_num:02d}"
                elif 'Tháng' in raw:
                    match = re.search(r'\d+', raw)
                    month_num = int(match.group())
                    return f"{base_year}-{month_num:02d}"
        
        df_kpi_new['Date_Key'] = df_kpi_new.apply(transform_year_key, axis=1, args=(set_year_kpi,))
        # chuẩn hóa lại tên các VP trong file
        key_rename_column = {
            'Phòng Bán Buôn HN' : 'Bán Buôn HN',
            'Phòng Beauty HN' : 'Beauty HN',
            'Phòng KD Tỉnh' : 'Phòng KDT' 
        }
        df_kpi_new['Phòng ban'] = df_kpi_new['Phòng ban'].replace(key_rename_column)


        # Có 4 sheet tương ứng với 4 phòng kinh doanh
        def melt_kpi_employees(df):
            df = df.melt(id_vars = ['Phòng ban', 'Tên nhân viên'],
                         var_name = 'Month_Raw',
                         value_name = 'Target'
                         )
            return df 

        # Phòng Bán Buôn HN, Phòng Beauty HN, Phòng kinh doanh 1, Phòng kinh doanh 2 , sau khi đã clean ở file Excel có các cột tương đương phòng tổng 
        # Các cột là Phòng ban để lookupvalue() trong BPI, bao gồm các cột 'Phòng ban' & 'Tên nhân viên' & 'Tổng'

        sheet_names = ['Phòng Bán Buôn HN', 'Phòng Beauty HN', 'Phòng kinh doanh 1', 'Phòng kinh doanh 2']
        dfs = []
        for sheet_name in sheet_names:
            df = pd.read_excel(path, sheet_name= sheet_name)
            df = melt_kpi_employees(df)
            df['Date_Key'] = df.apply(transform_year_key,axis=1, args=(set_year_kpi,))
            # df['Phòng ban'] = df['Phòng ban'].replace(key_rename_column)
            dfs.append(df)

        df_kpi_employee = pd.concat(dfs, ignore_index=True)

        # trả về data của tổng phòng, 
        return df_kpi_new, df_kpi_employee
    
    # Lấy dataframe từ hàm transform
    df_kpi, df_kpi_employees = transform_kpi_data(path_KPI)[0], transform_kpi_data(path_KPI)[1]
    # Load vào Postgres 
    load_to_postgres_optimized(df_kpi, table_name = 'kpi_misa_orders_2026', schema_name='stg')
    load_to_postgres_optimized(df_kpi_employees, table_name = 'kpi_misa_employees_2026', schema_name='stg')
    
        

if __name__ == "__main__":
    # Lấy thư mục gốc của dự án 
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Định nghĩa thư mục chứa dữ liệu DMS RAW
    DMS_RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'dms')

    # tạo engine để push data vào Postgres
    engine = get_engine()

    # Chạy 3 tác vụ phía trên (bỏ đa luồng đi vì nó silent lỗi)
    # with ThreadPoolExecutor(max_workers=3) as executor:
    task_process_dms(DMS_RAW_DIR, engine) # hàm cần vào là link PATH với engine đi kèm
    task_process_misa(BASE_DIR,engine=engine)
    task_process_others(BASE_DIR, engine)

    # SAU KHI XONG HẾT MỚI CHẠY SQL TẠO BẢNG INT (Vì bảng INT cần dữ liệu từ STG)
    print("\n  Đang tạo các bảng INT...")
    sql_files = [
        'create_int_products.sql',
        'create_int_employees.sql',
        'create_int_customers.sql'
    ]
    for sql_f in sql_files:
        execute_sql_file(os.path.join(BASE_DIR, 'postgres', sql_f), engine)
        
    print("✔️ TẤT CẢ QUÁ TRÌNH HOÀN TẤT!")


