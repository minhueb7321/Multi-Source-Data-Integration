import pandas as pd
import numpy as np
from scr.extract import fetch_dms_chi_tiet_ghe_tham, fetch_dms_tong_quan_ghe_tham, fetch_dms_file_khach_hang_tiem_nang
from scr.db_utils import load_to_postgres_optimized


# TÍNH TOÁN CHECK IN - OUT CHO NHÂN VIÊN - TRÊN FILE CHI TIẾT GHÉ THĂM
def transform_data_check_in_out(file_path, start_date=None, end_date=None):
    """
    Lấy data từ file chi tiết ghé thăm

    Trả về 3 file tính toán: (1) check in out, (2) thời gian, (3) khách hàng nắm giữ của từng người
    """
#  -----------------------------------------------------------------
#  ---------------------XỬ LÝ DỮ LIỆU-------------------------------
    # Đọc dữ liệu
    df = fetch_dms_chi_tiet_ghe_tham(file_path=file_path)
    # Tách cột thời điểm ghé thăm
    df[['check_in', 'check_out']] = df['Thời điểm ghé thăm'].str.split(' - ', expand=True)

    # Điều kiện tính toán
    df['Y'] = ((df['check_in'].notna()) & (df['check_out'].notna()) & (df['check_out'] != '')).astype(int)
    df['N'] = ((df['check_in'].notna()) & ((df['check_out'].isna()) | (df['check_out'] == ''))).astype(int)

    # Thay đổi giá trị của VP Hưng Yên thành VP Hải Dương do sát nhập VP
    map_value = {"VP Hưng Yên" : "VP Hải Dương"}
    df['Phòng ban'] = df['Phòng ban'].replace(map_value)

    # Loại bỏ các giá trị ở cột VP để làm sạch dữ liệu
    mask_value = ['Hỗ trợ vận hành', 'Vận hành', 'Giao nhận','Digital', 'Marketing', 'Kế toán Hà Nội', 'Kỹ thuật', 'Hành chính nhân sự', 'Ban Giám Đốc'] # cần thêm giá trị
    df = df[~df['Phòng ban'].isin(mask_value)]
    

#  -----------------------------------------------------------------
#  ---------------------TÍNH TOÁN CHECK IN OUT----------------------
    
    # Tính toán số lượng check in check out
    data_check_in = df.groupby(['Phòng ban', 'Mã nhân viên', 'Nhân viên ghé thăm']).agg({'Y': 'sum', 'N': 'sum'}).reset_index()



#  -----------------------------------------------------------------
#  --------------------TÍNH TOÁN SỐ KHÁCH HÀNG THỰC-----------------

    # Tính toán số khách hàng duy nhất (unique) của từng nhân viên thăm
    distinct_count_customers = df.groupby(['Phòng ban','Mã nhân viên', 'Nhân viên ghé thăm'])['Mã Khách hàng/Tiềm năng'].nunique().reset_index()


#  -----------------------------------------------------------------
#  ------------------TÍNH TOÁN THỜI GIAN THĂM KHÁCH-----------------

    # Chuyển string sang timestamp để tính toán khoảng thời gian
    # Tính toán check in time
    df['check_in_time'] = pd.to_datetime(df['check_in'], format = '%H:%M:%S', errors='coerce')
    # Tính toán check out time
    df['check_out_time'] = pd.to_datetime(df['check_out'], format = '%H:%M:%S', errors='coerce')
    # Tính toán số phút mà nhân viên thăm mỗi khách
    df['duration_min'] = (df['check_out_time'] - df['check_in_time']).dt.total_seconds() / 60
    # nếu thăm khách >60 phút => trả về 60 phút (max 60')
    df['duration_min'] = np.where(df['duration_min'] > 60, 60, df['duration_min'])

    # Tính toán đếm số lần thăm và tổng thời gian thăm của từng NV
    time_calc_and_customers = df.groupby(['Phòng ban', 'Mã nhân viên', 'Nhân viên ghé thăm']).agg({'Mã Khách hàng/Tiềm năng': 'count', 'duration_min': 'sum'}).reset_index()

    # Tính toán bình quân thời gian (để check trên sheet)
    time_calc_and_customers['time_avg'] = time_calc_and_customers['duration_min'] / time_calc_and_customers['Mã Khách hàng/Tiềm năng']


    return data_check_in,distinct_count_customers,time_calc_and_customers
#  -----------------------------------------------------------------
#  ----------------------------DONE---------------------------------



# TÍNH TOÁN BÌNH QUÂN GHÉ THĂM VÀ SỐ KILOMETERS - TRÊN FILE TỔNG QUAN GHÉ THĂM
def customers_potential_km_calc(file_path):
    """
    Đẩy file tổng quan ghé thăm

    File trả về gồm 2 file : trung bình số khách ghé thăm(1) , sô kilometers(2)
    """
#  -----------------------------------------------------------------
#  ---------------------XỬ LÝ DỮ LIỆU-------------------------------
    # Đọc dữ liệu
    df = fetch_dms_tong_quan_ghe_tham(file_path=file_path)

    # Lọc các cột cần lấy
    df = df[['Tên nhân viên', 'Mã nhân viên', 'Phòng ban', 'Tổng KH ghé thăm', 'Tổng số lần ghé thăm KH', 'Tổng TN ghé thăm', 'Tổng số lần ghé thăm TN', 'Tổng km di chuyển']]
    # Loại bỏ cách dong cí giá trị trong cột `Phong ban`
    value_to_remove = ['Hỗ trợ vận hành', 'Vận hành', 'Giao nhận','Digital', 'Marketing', 'Kế toán Hà Nội', 'Kỹ thuật', 'Hành chính nhân sự', 'Ban Giám Đốc']
    df = df[~df['Phòng ban'].isin(value_to_remove)]
    # load_to_postgres_optimized(df=df, table_name='test_tong_quan_ghe_tham', schema_name= 'stg')
#  -----------------------------------------------------------------
#  ------------------TÍNH TOÁN BÌNH QUÂN----------------------------

    # Tính toán bình quân số lần ghé thăm
    df['TB lượt ghé thăm/ Khách hàng'] = np.where(df['Tổng KH ghé thăm'] == 0 , 0, df['Tổng số lần ghé thăm KH'] / df['Tổng KH ghé thăm'])
    df['TB lượt ghé thăm/ Tiềm năng'] = np.where(df['Tổng TN ghé thăm'] == 0 , 0, df['Tổng số lần ghé thăm TN'] / df['Tổng TN ghé thăm'])
    # Xuất data dữ liệu
    avg_kh_tn = df[['Tên nhân viên', 'Mã nhân viên', 'Phòng ban', 'TB lượt ghé thăm/ Khách hàng', 'TB lượt ghé thăm/ Tiềm năng']]


#  -----------------------------------------------------------------
#  ------------------TÍNH TOÁN KILOMETERS---------------------------

    # Xuất data dữ liệu
    kilometer_df = df[['Tên nhân viên', 'Mã nhân viên', 'Phòng ban', 'Tổng km di chuyển']]
    
#  -----------------------------------------------------------------
#  ----------------------------DONE---------------------------------

    return avg_kh_tn, kilometer_df



# TÍNH TOÁN MỞ MỚI TÀI KHOẢN THEO NHÂN VIÊN
def calc_account_status(file_path_kh, file_path_tn ,month_to_extract, year_to_extract = 2026, path_dms_employees = None):
#  -----------------------------------------------------------------
#  ----------------------Đọc data file khách hàng-------------------
    # Đọc data file khách hàng
    # df_kh = pd.read_excel(file_path_kh)
    df_kh, df_tn = fetch_dms_file_khach_hang_tiem_nang(file_path_kh=file_path_kh,file_path_tn=file_path_tn)

    # Lọc các cột cần lấy
    df_kh = df_kh[['Mã khách hàng', 'Tên khách hàng', 'Người phụ trách', 'Trạng thái thông tin', 'Ngày chuyển đổi', 'Phòng ban']]
    # Lấy data từ các chi nhanh cần thiết
    values_to_take = ['VP Sài Gòn', 'VP Cần Thơ', 'VP Vinh', 'VP Hưng Yên','Beauty HN', 'VP Đắk Lắk', 'VP Hải Phòng', 'VP Đà Nẵng','VP Nam Định', 'VP Đồng Nai', 'VP Hải Dương', 'VP Nha Trang','Bán buôn HN', 'KD3', 'VP Thanh Hóa', 'Phòng KDT','VP Thái Nguyên' ]
    df_kh = df_kh[df_kh['Phòng ban'].isin(values_to_take)]
    # Thay đổi giá trị văn phòng
    map_value = {"VP Hưng Yên" : "VP Hải Dương"}
    df_kh['Phòng ban'] = df_kh['Phòng ban'].replace(map_value)
    # Chuyển cột ngày chuyển đổi sang timestamp
    df_kh['Ngày chuyển đổi'] = pd.to_datetime(df_kh['Ngày chuyển đổi'], format='%H:%M:%S %d/%m/%Y')
    # Lọc những ngày chuyển đổi trong tháng cần lấy, ví dụ tháng 3 năm 2026 , có tham số cần điền trong hàm def
    df_kh = df_kh[(df_kh['Ngày chuyển đổi'].dt.year == year_to_extract) & (df_kh['Ngày chuyển đổi'].dt.month == month_to_extract)]


#  -----------------------------------------------------------------
#  ----------------------------XUẤT FILE----------------------------
    # Xuất data chờ duyệt
    KH_pending = df_kh[df_kh['Trạng thái thông tin'] == 'Chờ duyệt']
    KH_pending = pd.pivot_table(
        KH_pending,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã khách hàng'],
        aggfunc={
            'Mã khách hàng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()

    # Xuất data đã duyệt
    KH_accepted = df_kh[df_kh['Trạng thái thông tin'] == 'Đã duyệt']
    KH_accepted = pd.pivot_table(
        KH_accepted,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã khách hàng'],
        aggfunc={
            'Mã khách hàng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()

    # Xuất data từ chối
    KH_cancelling = df_kh[df_kh['Trạng thái thông tin'] == 'Từ chối']
    # cancelling.groupby(['Phòng ban', 'Người phụ trách'])['Mã khách hàng'].count()
    KH_cancelling = pd.pivot_table(
        KH_cancelling,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã khách hàng'],
        aggfunc={
            'Mã khách hàng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()

#  -----------------------------------------------------------------
#  ----------------------Đọc data file tiềm năng-------------------

    # Lọc các cột cần lấy
    df_tn = df_tn[['Mã tiềm năng', 'Tên tiềm năng', 'Người phụ trách', 'Trạng thái', 'Ngày tạo', 'Phòng ban']]
    # Lấy data từ các chi nhanh cần thiết
    values_to_take = ['VP Sài Gòn', 'VP Cần Thơ', 'VP Vinh', 'VP Hưng Yên','Beauty HN', 'VP Đắk Lắk', 'VP Hải Phòng', 'VP Đà Nẵng','VP Nam Định', 'VP Đồng Nai', 'VP Hải Dương', 'VP Nha Trang','Bán buôn HN', 'KD3', 'VP Thanh Hóa', 'Phòng KDT','VP Thái Nguyên' ]
    df_tn = df_tn[df_tn['Phòng ban'].isin(values_to_take)]
    # Thay đổi giá trị văn phòng
    map_value = {"VP Hưng Yên" : "VP Hải Dương"}
    df_tn['Phòng ban'] = df_tn['Phòng ban'].replace(map_value)
    # Chuyển cột ngày chuyển đổi sang timestamp
    df_tn['Ngày tạo'] = pd.to_datetime(df_tn['Ngày tạo'], format='%H:%M:%S %d/%m/%Y')
    # Lọc những ngày chuyển đổi trong tháng cần lấy, ví dụ tháng 3 năm 2026 , có tham số cần điền trong hàm def
    df_tn = df_tn[(df_tn['Ngày tạo'].dt.year == year_to_extract) & (df_tn['Ngày tạo'].dt.month == month_to_extract)]


    # Xuất data chờ duyệt
    TN_pending = df_tn[df_tn['Trạng thái'] == 'Chờ duyệt']
    TN_pending = pd.pivot_table(
        TN_pending,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã tiềm năng'],
        aggfunc={
            'Mã tiềm năng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()

    # Xuất data đã duyệt
    TN_accepted = df_tn[df_tn['Trạng thái'] == 'Đã duyệt']
    TN_accepted = pd.pivot_table(
        TN_accepted,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã tiềm năng'],
        aggfunc={
            'Mã tiềm năng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()


    # Xuất data từ chối
    TN_cancelling = df_tn[df_tn['Trạng thái'] == 'Từ chối']
    TN_cancelling = pd.pivot_table(
        TN_cancelling,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã tiềm năng'],
        aggfunc={
            'Mã tiềm năng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()

    df_dms_employees = pd.read_excel(path_dms_employees, usecols= ['Mã Nhân Viên', 'Phòng ban', 'Tên Nhân Viên'])
    df_dms_employees['Phòng ban'] = df_dms_employees['Phòng ban'].replace(map_value)
    df_dms_employees['UID'] = df_dms_employees['Tên Nhân Viên'].astype(str) + '_' + df_dms_employees['Phòng ban'].astype(str)
    
    dfs = [KH_pending, KH_accepted, KH_cancelling , TN_pending, TN_accepted, TN_cancelling]
    for i in range(len(dfs)):
        if i <= 2:
            dfs[i]['UID'] =  dfs[i]['Người phụ trách'].astype(str) + '_' + dfs[i]['Phòng ban'].astype(str)
            dfs[i] = pd.merge(left = dfs[i], right = df_dms_employees, how = 'left', right_on = 'UID', left_on = 'UID')
            dfs[i] = dfs[i][['Phòng ban_x', 'Mã Nhân Viên', 'Người phụ trách', 'Mã khách hàng']]
            dfs[i] = dfs[i].rename(columns={'Phòng ban_x': 'Phòng ban'})
        else:
            dfs[i]['UID'] =  dfs[i]['Người phụ trách'].astype(str) + '_' + dfs[i]['Phòng ban'].astype(str)
            dfs[i] = pd.merge(left = dfs[i], right = df_dms_employees, how = 'left', right_on = 'UID', left_on = 'UID')
            dfs[i] = dfs[i][['Phòng ban_x', 'Mã Nhân Viên', 'Người phụ trách', 'Mã tiềm năng']]
            dfs[i] = dfs[i].rename(columns={'Phòng ban_x': 'Phòng ban'})

    KH_pending, KH_accepted, KH_cancelling , TN_pending, TN_accepted, TN_cancelling = dfs[0], dfs[1], dfs[2], dfs[3], dfs[4], dfs[5]
    # Trả về 3 file KH dạng pivot
    return KH_pending, KH_accepted, KH_cancelling , TN_pending, TN_accepted, TN_cancelling



from scr.extract import fetch_dms_employees
def lightning_transform_employees(file_path):
    df = fetch_dms_employees(file_path)
    return df


def fetch_data_chi_tiet_ghe_tham(file_path, start_date=None, end_date=None):

    # Đọc dữ liệu
    df = fetch_dms_chi_tiet_ghe_tham(file_path=file_path)
    # Tách cột thời điểm ghé thăm
    df[['check_in', 'check_out']] = df['Thời điểm ghé thăm'].str.split(' - ', expand=True)

    # Điều kiện tính toán
    df['Y'] = ((df['check_in'].notna()) & (df['check_out'].notna()) & (df['check_out'] != '')).astype(int)
    df['N'] = ((df['check_in'].notna()) & ((df['check_out'].isna()) | (df['check_out'] == ''))).astype(int)
    df['check_in_time'] = pd.to_datetime(df['check_in'], format = '%H:%M:%S', errors='coerce')
    # Tính toán check out time
    df['check_out_time'] = pd.to_datetime(df['check_out'], format = '%H:%M:%S', errors='coerce')
    # Tính toán số phút mà nhân viên thăm mỗi khách
    df['duration_min'] = (df['check_out_time'] - df['check_in_time']).dt.total_seconds() / 60
    # nếu thăm khách >60 phút => trả về 60 phút (max 60')
    df['duration_min'] = np.where(df['duration_min'] > 60, 60, df['duration_min'])
    # Thay đổi giá trị của VP Hưng Yên thành VP Hải Dương do sát nhập VP
    map_value = {"VP Hưng Yên" : "VP Hải Dương"}
    df['Phòng ban'] = df['Phòng ban'].replace(map_value)

    # Loại bỏ các giá trị ở cột VP để làm sạch dữ liệu
    mask_value = ['Hỗ trợ vận hành', 'Vận hành', 'Giao nhận','Digital', 'Marketing', 'Kế toán Hà Nội', 'Kỹ thuật', 'Hành chính nhân sự', 'Ban Giám Đốc'] # cần thêm giá trị
    df = df[~df['Phòng ban'].isin(mask_value)]
    return df


def fetch_data_tong_quan_ghe_tham(file_path):
    """
    Đẩy file tổng quan ghé thăm

    File trả về gồm 2 file : trung bình số khách ghé thăm(1) , sô kilometers(2)
    """
#  -----------------------------------------------------------------
#  ---------------------XỬ LÝ DỮ LIỆU-------------------------------
    # Đọc dữ liệu
    df = fetch_dms_tong_quan_ghe_tham(file_path=file_path)

    # Lọc các cột cần lấy
    df = df[['Tên nhân viên', 'Mã nhân viên', 'Phòng ban', 'Tổng KH ghé thăm', 'Tổng số lần ghé thăm KH', 'Tổng TN ghé thăm', 'Tổng số lần ghé thăm TN', 'Tổng km di chuyển']]
    # Loại bỏ cách dong cí giá trị trong cột `Phong ban`
    value_to_remove = ['Hỗ trợ vận hành', 'Vận hành', 'Giao nhận','Digital', 'Marketing', 'Kế toán Hà Nội', 'Kỹ thuật', 'Hành chính nhân sự', 'Ban Giám Đốc']
    df = df[~df['Phòng ban'].isin(value_to_remove)]
    df['TB lượt ghé thăm/ Khách hàng'] = np.where(df['Tổng KH ghé thăm'] == 0 , 0, df['Tổng số lần ghé thăm KH'] / df['Tổng KH ghé thăm'])
    df['TB lượt ghé thăm/ Tiềm năng'] = np.where(df['Tổng TN ghé thăm'] == 0 , 0, df['Tổng số lần ghé thăm TN'] / df['Tổng TN ghé thăm'])
    

    # Xuất data dữ liệu
    return df