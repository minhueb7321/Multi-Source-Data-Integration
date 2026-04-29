import pandas as pd ; import os ; import openpyxl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

def get_tong_hop_files_year(base_path, year):
    """
    # T*_{year}/*Tong_Hop*.xlsx  , ở đây nó tìm folder dạng T2_2026 -> tiến vào file có bao gồm *Tong_Hop* và đuôi .xlsx
    Nghĩa là:
    Folder bắt đầu bằng T
    có chứa _2026 (ví dụ)
    bên trong có file chứa "Tong_Hop"
    đuôi .xlsx
    T1_2026/Tong_Hop_Sale.xlsx
    T2_2026/ABC_Tong_Hop.xlsx
    """
    base_path = Path(base_path)
    files = list(base_path.glob(f"T*_{year}/*Tong_Hop*.xlsx")) # glob là hàm để tìm kiếm tệp dựa trên wildcard - ký tự đại diện
    # loại file tạm , mở file Excel nên là tự sinh ra file tạm này
    files = [f for f in files if not f.name.startswith("~$")]
    return files # trả về list của các file dạng TONG_HOP.xlsx

def read_excel_file(file_path):
    """Hàm phụ trợ để đọc file đơn lẻ"""
    try:
        # Có thể thêm các ETL , nhưng file này đã sạch r
        return pd.read_excel(file_path)
    except Exception as e :
        print(f"Lỗi khi đọc file {file_path}: {e}")
        return None


def load_tong_hop(base_path, year):
    """
    đẩy path data\processed\misa\ và năm để lấy các file
    """
    files = get_tong_hop_files_year(base_path, year=year)
    
    if not files:
        print(f"Không tìm thấy file nào cho năm {year}")
        return pd.DataFrame()

    # Sử dụng ThreadPoolExecutor để đọc file song song
    with ThreadPoolExecutor(max_workers=5) as executor: # cho 12 tháng
        # map sẽ áp dụng hàm read_excel_file cho từng item trong list files
        results = list(executor.map(read_excel_file, files))
    
    # Loại bỏ các kết quả None nếu có lỗi trong quá trình đọc
    dfs = [df for df in results if df is not None]
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()
    

def concatenate_file_end_xlsx(path):
    dfs = []
    for file in os.listdir(path):
        if file.endswith('.xlsx') and not file.startswith('~$'):
   
            df = pd.read_excel(os.path.join(path, file) , usecols= ["serial",	"product_name"	,"bundle_type_name"	,"total_stamp"	,"batch_number",	"expire_time",	"produce_time"], engine='openpyxl')
            df['department'] = file
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

