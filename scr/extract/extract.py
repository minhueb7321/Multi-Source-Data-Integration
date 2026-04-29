import pandas as pd
import warnings 
warnings.filterwarnings('ignore')


# ---------------MISA----------------

# Riêng cho MISA
def fetch_misa_data(file_path):
    # Đọc data , cột tính từ dòng thứ 3
    df = pd.read_excel(file_path, header=2, sheet_name='Báo cáo')
    # Loại bỏ dòng cuối
    df = df.iloc[:-1].copy()
    
    return df
# Nhân sự trên Misa
def fetch_misa_employees(file_path):
    # Truyền engine xlrd vào 
    df = pd.read_excel(file_path, header=1, engine='xlrd')
    # Loại bỏ dòng cuối
    df = df.iloc[:-1]
    # Lấy các cột cần lấy 
    # df  =df[['Mã nhân viên', 'Tên nhân viên']]
    return df

# Khách hàng trên misa
def fetch_misa_customers(file_path):
    # Lấy dòng 1 làm header, bỏ dòng cuối
    df = pd.read_excel(file_path, header=1)
    df = df.iloc[:-1]
    return df


# Sản phẩm trên Misa
def fetch_misa_products(file_path):
    # Lấy dòng 1 làm header, bỏ dòng cuối
    df = pd.read_excel(file_path, header=1)
    df = df.iloc[:-1]
    return df


# ---------------DMS----------------


# Nhân sự trên DMS
def fetch_dms_employees(file_path):
    df = pd.read_excel(file_path)
    # Lấy các cột cần thiết

    df.columns = [col.capitalize() for col in df.columns]
    # df = df[['Mã nhân viên', 'Tên nhân viên', 'Phòng ban', 'Chức danh']]
    return df

# Riêng cho DMS

def fetch_dms_chi_tiet_ghe_tham(file_path):
    # Đọc đối với báo cáo chi tiết ghé thăm DMS
    df = pd.read_excel(file_path, header=2)
    return df 


def fetch_dms_tong_quan_ghe_tham(file_path):
    # Đọc file đối với báo cáo tổng quan ghé thăm DMS
    df = pd.read_excel(file_path, header=2) # lấy dòng 3 làm cột chính
    # Lọc bỏ dòng đầu
    df = df.iloc[1:]
    return df


def fetch_dms_file_khach_hang_tiem_nang(file_path_kh:str, file_path_tn:str) -> pd.DataFrame:
    # Đọc file khách hàng
    kh = pd.read_excel(file_path_kh)
    # Đọc file tiềm năng
    tn = pd.read_excel(file_path_tn)
    return kh, tn


def fetch_dms_file_khach_hang_tiem_nang_chung(
        file_path_kh:str, 
        file_path_tn:str, 
        file_path_kh_chung:str, 
        file_path_tn_chung:str) -> pd.DataFrame:
    # Đọc file khách hàng
    kh = pd.read_excel(file_path_kh)
    # Đọc file tiềm năng
    tn = pd.read_excel(file_path_tn)
    # Đọc file kh chung
    kh_chung = pd.read_excel(file_path_kh_chung)
    # Đọc file tn chung
    tn_chung = pd.read_excel(file_path_tn_chung)
    return kh, tn, kh_chung, tn_chung

def fetch_dms_file_products(file_path:str):
    df = pd.read_excel(file_path)
    return df



# -----------OTHERS------------