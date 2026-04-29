import os
import pandas as pd

# Module xử lý trên MISA
from scr.transform_misa_provinces import process_misa_file_provinces
from scr.transform_misa_ha_noi import process_misa_file_HN
from scr.transform_misa_employees import concatnate_all_employees
from scr.transform_misa_customers import concatenate_all_customers
from scr.db_utils import load_to_postgres_optimized
# Module de gop cac file tong hop lai
from scr.concatenate_file import load_tong_hop
from concurrent.futures import ThreadPoolExecutor
from config_dict import CONFIG_HN_DETAIL,CONFIG_PROVINCES


def process_hn_data(BASE_FILE_HN, CONFIG_HN_DETAIL, df_all_employees, period, OUTPUT_FOLDER_HANOI):
    #  ---------------------------XỬ LÝ HÀ NỘI----------------------------
    if os.path.exists(BASE_FILE_HN):
        df_hn = process_misa_file_HN(BASE_FILE_HN, config_hn=CONFIG_HN_DETAIL, file_employees=df_all_employees)
        file_name_individual = f"Misa_Ha_Noi_{period}.xlsx"
        df_hn.to_excel(os.path.join(OUTPUT_FOLDER_HANOI, file_name_individual), index=False)
        print(f"✅ Xử lý thành công Misa: Hà Nội ({len(df_hn)} dòng , {len(df_hn.columns)} cột)\n")
        return df_hn
    else:
        print("❌ Cảnh báo: Chưa có file Ha Noi.xlsx trong thư mục raw để xử lý.")
        return pd.DataFrame()


def process_provinces_data(CONFIG_PROVINCES, RAW_FOLDER_PROVINCES,df_all_employees, period, OUTPUT_FOLDER_PROVINCES):
    #  ---------------------------XỬ LÝ FILE TỈNH--------------------------
    province_dfs = [] # tạo df tổng để gộp lại file to các tính để gộp với Hà Nội
    for province, info in CONFIG_PROVINCES.items():
        file_path = os.path.join(RAW_FOLDER_PROVINCES, info['file_name'])
        if os.path.exists(file_path):
            df_p = process_misa_file_provinces(file_path=file_path,province_name= province,df_employees= df_all_employees,config_periods= info['periods'])
            # Xuất file riêng
            file_name = f"Misa_{province.replace(' ', '_')}_{period}.xlsx"
            df_p.to_excel(os.path.join(OUTPUT_FOLDER_PROVINCES, file_name), index=False)
            province_dfs.append(df_p)
            print(f"✅ Xử lý thành công Misa: {province} ({len(df_p)} dòng , {len(df_p.columns)} cột) \n")
        else:
            print(f"❌ Cảnh báo: Không tìm thấy file cho {province} tại đường dẫn: {file_path}\n")
    return pd.concat(province_dfs, ignore_index=True) if province_dfs else pd.DataFrame()


if __name__ == "__main__":
    # Lấy path của thư mục gốc
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Set up ngày tháng để chạy
    MONTH, YEAR = 4, 2026
    period = f"T{MONTH}_{YEAR}"

    # Xử lý nhân sự trước để lấy df_all_employees làm input cho các hàm sau
    RAW_EMPLOYEE_FOLDER = os.path.join(BASE_DIR, 'data', 'raw', 'misa', 'employees')
    df_all_employees = concatnate_all_employees(RAW_EMPLOYEE_FOLDER)    

    # Clean data nhân sự , để ý nếu nhân sự có 2 tên trùng trong 1 phòng ban cần để ý hỏi IT 
    df_all_employees = df_all_employees[df_all_employees['Mã nhân viên'] != 'HN_VH_NGUYENQUANGQUYEN1']


    # Dùng phân luồng chạy song song cả file Hà Nội và các file tỉnh
    print(f"\n🔰Đang bắt đầu xử lý song song Hà Nội và các Tỉnh cho kỳ {period}...")
    # Path của Hà Nội
    BASE_FILE_HN = os.path.join(BASE_DIR, 'data', 'raw', 'misa', period, 'Ha Noi.xlsx')

    # Folder của các tỉnh
    RAW_FOLDER_PROVINCES = os.path.join(BASE_DIR, 'data', 'raw', 'misa', period)

    # Output cho cả Hà Nội và các file tỉnh
    OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'processed', 'misa', period)

    # Không có tự tạo mục dạng T3_2026, period = f"T{MONTH}_{YEAR}"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Phân luồng chạy tác vụ của cả 2 hàm Hà Nội và các tỉnh
    # with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit 2 task vào 2 luồng khác nhau
    df_hn_final = process_hn_data(   # hà nội
                                    BASE_FILE_HN = BASE_FILE_HN, 
                                    CONFIG_HN_DETAIL = CONFIG_HN_DETAIL, 
                                    df_all_employees = df_all_employees, 
                                    period = period , 
                                    OUTPUT_FOLDER_HANOI = OUTPUT_DIR)
    df_provinces_final = process_provinces_data(  # các tỉnh
                                           CONFIG_PROVINCES = CONFIG_PROVINCES, 
                                           RAW_FOLDER_PROVINCES = RAW_FOLDER_PROVINCES, 
                                           df_all_employees = df_all_employees, 
                                           period = period, 
                                           OUTPUT_FOLDER_PROVINCES = OUTPUT_DIR)

        # Lấy kết quả và gán
        # df_hn_final = future_hn.result()
        # df_provinces_final = future_provinces.result()

    # Gộp 2 dataframe vào làm 1 file lớn của cả tháng
    all_dataframes = [df_hn_final, df_provinces_final]
    valid_dfs = [d for d in all_dataframes if d is not None and not d.empty]

    # Kiểm tra xem có DF nào để gộp không
    if valid_dfs:
        df_final = pd.concat(valid_dfs , ignore_index=True)
    else :
        df_final = pd.DataFrame() # Tạo DF rỗng

    # Nếu có df thì chạy hàm
    if not df_final.empty:
        # Xuất file tổng hợp kỳ đang chạy
        total_path = os.path.join(OUTPUT_DIR, f"Misa_Tong_Hop_Full_{period}.xlsx")
        df_final.to_excel(total_path, index=False)
        
        # Gộp tổng hợp năm và đẩy database
        BASE_ALL_FILE_DIR = os.path.join(BASE_DIR, 'data', 'processed', 'misa')
        df_year = load_tong_hop(BASE_ALL_FILE_DIR, year=YEAR)
        df_year = df_year[df_year['Năm chốt công nợ'] == YEAR]

        # In kết quả để check
        print(f'✅ Gộp file tổng hợp năm {YEAR} thành công, số dòng: {len(df_year)}, số cột:{len(df_year.columns)}, doanh thu gộp : {df_year["Tổng tiền thanh toán"].sum()}, doanh thu ròng : {df_year["Doanh thu ròng"].sum()} \n')

        # Đẩy vào Postgres
        load_to_postgres_optimized(df_year, table_name=f'misa_sct{YEAR}', schema_name='stg')
        print(f"\n✅ Đã xử lý xong toàn bộ và đẩy vào Database!")
    else :
        print("Không có data hoặc thư mục không tồn tại")