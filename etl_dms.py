import os
import pandas as pd
# Module xử lý trên DMS
from scr.transform_dms_employees import transform_data_check_in_out, calc_account_status,customers_potential_km_calc
from scr.transform_dms_customers import tinh_toan_kh_kho_chung
from scr.transform_dms_employees import fetch_data_chi_tiet_ghe_tham, fetch_data_tong_quan_ghe_tham

# Module để export file Excel và đẩy lên Postgres
from scr.load import load_file_excel
from scr.db_utils import load_to_postgres_optimized

# Module để xử lý đa luồng
from concurrent.futures import ThreadPoolExecutor

# Module để tạo engine và tránh lỗi tạo engine
from sqlalchemy import create_engine;from urllib.parse import quote_plus



# Tạo engine
def get_engine():
    # Chuỗi kết nối Postgres: postgresql://user:password@host:port/dbname
    connection_str = f"postgresql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASS'))}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(connection_str, pool_size=10, max_overflow=20)


# Hàm export dms (cần phát triển xuất ra mã nhân viên để kiểm soát chặt chẽ hơn so với nối bằng tên nhân viên trên file)
def export_data_dms(DMS_RAW_DIR, FILE_EXPORT_EMPLOYEES, MONTH, YEAR, path_dms_employees = None):
    # Liệt kê các path raw để truyền vào đọc file
    FILE_CTGT = os.path.join(DMS_RAW_DIR, 'MYDICO_Báo cáo chi tiết ghé thăm.xlsx') # chi tiết
    FILE_THGH = os.path.join(DMS_RAW_DIR, 'MYDICO_Báo cáo tổng hợp ghé thăm.xlsx') # tổng hợp
    FILE_KH   = os.path.join(DMS_RAW_DIR, 'Danh sách khách hàng.xlsx') # khách hàng
    FILE_TN   = os.path.join(DMS_RAW_DIR, 'Danh sách tiềm năng.xlsx')  # tiềm năng
    FILE_KH_CHUNG = os.path.join(DMS_RAW_DIR, 'Danh sách khách hàng chung.xlsx')  # khách hàng chung
    FILE_TN_CHUNG = os.path.join(DMS_RAW_DIR, 'Danh sách tiềm năng chung.xlsx') # tiềm năng chung



    # (1). Xuất data tính toán check in, out(1), số khách hàng đang nắm(2), thời gian thăm khách(3)
    stage1_results = transform_data_check_in_out(file_path=FILE_CTGT)
    names = ["_CHECK_IN_CHECK_OUT", "_UNIQUE_CUSTOMERS", "_TIME_CALC" ]
    for df_stage1_results, name in zip(stage1_results, names):
        load_file_excel(df_stage1_results, file_path=FILE_EXPORT_EMPLOYEES, custom_name=name)


    # (2). Xuất data tính toán số khách ghé thăm + kilometers
    stage2_results = customers_potential_km_calc(file_path=FILE_THGH)
    names = ["_AVG_KH_TN", "_KILOMETERS"]
    for df_stage2_results, name in zip(stage2_results, names):
        load_file_excel(df_stage2_results, file_path=FILE_EXPORT_EMPLOYEES,custom_name= name)


    # (3). Xuất data trạng thái tài khoản từng người 
    month = MONTH # chỉnh month
    year = YEAR # chỉnh year ở hàm ngoài
    stage3_results = calc_account_status(file_path_kh=FILE_KH,file_path_tn=FILE_TN, month_to_extract=month, year_to_extract=year, path_dms_employees=path_dms_employees)
    names = ["__KH_pending", "__KH_accepted", "__KH_cancelling", "__TN_pending", "__TN_accepted", "__TN_cancelling"]
    for df_stage3_results, name in zip(stage3_results, names):
        load_file_excel(df_stage3_results, file_path=FILE_EXPORT_EMPLOYEES, custom_name= name)


    # (4). Xuất data tổng KH và tổng tất cả KH và TN chung một thể
    stage4_results = tinh_toan_kh_kho_chung(file_name_kh=FILE_KH,
                                                file_name_kh_chung=FILE_KH_CHUNG,
                                                file_name_tn=FILE_TN,
                                                file_name_tn_chung=FILE_TN_CHUNG, path_dms_employees=path_dms_employees)
    names = ["__ALL_KH", "__ALL_KH_TN"]
    for df_stage4_results, name in zip(stage4_results, names):
        load_file_excel(df_stage4_results, file_path=FILE_EXPORT_EMPLOYEES, custom_name= name)


# Hàm export các file từ DMS vào Postgres Mydico-Staging Layer
def export_to_postgres(DMS_RAW_DIR, engine):
    """
    Luồng đẩy dữ liệu thô vào Postgres Staging
    """
    tasks = [
        ('orders/Mydico-Danh sách đơn hàng.xlsx', 'dms_orders_update', 'Ngày tạo', '%H:%M %d/%m/%Y'),  # Orders - insert update
        ('inventory/MYDICO_Danh sách phiếu nhập kho.xlsx', 'dms_inbound_update', 'Thời gian tạo', '%H:%M:%S %d/%m/%Y'), # kho vận - insert update
        ('inventory/MYDICO_Danh sách phiếu xuất kho.xlsx', 'dms_outbound_update', 'Thời gian tạo', '%H:%M:%S %d/%m/%Y'), # kho vận - insert update
        ('inventory/MYDICO_Danh sách vận đơn.xlsx', 'dms_lading_update', 'Ngày tạo', '%H:%M %d/%m/%Y'), # kho vận - insert update
        ('inventory/MYDICO_Lịch sử xuất nhập.xlsx', 'dms_inventory_history_update', 'Thời gian phát sinh', '%H:%M %d/%m/%Y'), # kho vận - lịch sử xuất nhập ,- insert update
        ('inventory/MYDICO_Thống kê sản phẩm trong kho.xlsx', 'dms_inventory', None, None) # kho vận - số kho
    ]   
    for rel_path, table, date_col, date_fmt in tasks:
        path = os.path.join(DMS_RAW_DIR, rel_path)
        df = pd.read_excel(path, parse_dates=[date_col] if date_col else None, date_format=date_fmt)
        load_to_postgres_optimized(df, table_name=table, schema_name='stg')


if __name__ == "__main__":
    # Lấy thư mục gốc của dự án 
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Định nghĩa thư mục chứa dữ liệu DMS RAW
    DMS_RAW_DIR_CUSTOMERS = os.path.join(BASE_DIR, 'data', 'raw', 'dms', 'customers')

    DMS_RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'dms')
    
    load_to_postgres_optimized(fetch_data_chi_tiet_ghe_tham(file_path=os.path.join(BASE_DIR, 'data', 'raw', 'dms','customers', 'MYDICO_Báo cáo chi tiết ghé thăm.xlsx')), table_name= 'test_chi_tiet_ghe_tham', schema_name='stg')

    load_to_postgres_optimized(fetch_data_tong_quan_ghe_tham(file_path=os.path.join(BASE_DIR, 'data', 'raw', 'dms','customers', 'MYDICO_Báo cáo tổng hợp ghé thăm.xlsx')), table_name= 'test_tong_quan_ghe_tham', schema_name='stg')

    
    # Định nghĩa thư mục trả data về ở file export employees
    FILE_EXPORT_EMPLOYEES = os.path.join(BASE_DIR, 'data', 'processed', 'dms', 'sale_operations')

    path_dms_employees = os.path.join(BASE_DIR, 'data', 'raw', 'dms', 'employees', 'Danh sách nhân viên.xlsx')

    # Định nghĩa ngày tháng để tiến hành tính toán ở hàm `export_data_dms`
    MONTH =4
    YEAR =2026

    # Tạo engine 
    engine = get_engine()

    export_data_dms(DMS_RAW_DIR = DMS_RAW_DIR_CUSTOMERS,  
                    FILE_EXPORT_EMPLOYEES = FILE_EXPORT_EMPLOYEES, 
                    MONTH = MONTH, 
                    YEAR = YEAR, 
                    path_dms_employees=path_dms_employees) # truyền các tham số vào submit
        # Luồng đẩy Postgres
    export_to_postgres(DMS_RAW_DIR = DMS_RAW_DIR,engine = engine)
    print("✔️ Đã hoàn thành song song cả 2 nhiệm vụ!")


    # from scr.concatenate_file import concatenate_file_end_xlsx
    # path = r'F:\Documents\ETL_MISA_DMS\data\raw\others\2026-20260420T100119Z-3-001\Kiểm kho T4-2026\Cũ'
    # df = concatenate_file_end_xlsx(path)
    # load_to_postgres_optimized(df =df, table_name='kiem_kho_cu_t4_2026', schema_name='stg')

    # path = r'F:\Documents\ETL_MISA_DMS\data\raw\others\2026-20260420T100119Z-3-001\Kiểm kho T4-2026\Mới'
    # df = concatenate_file_end_xlsx(path)
    # load_to_postgres_optimized(df =df, table_name='kiem_kho_moi_t4_2026', schema_name='stg')

