import pandas as pd
import numpy as np
from scr.extract.extract import fetch_misa_data

def process_misa_file_HN(file_path, config_hn, file_employees:pd.DataFrame):

    """
    Truyền vào file_path + tên vùng + ngày chốt công nợ 
    """
    # Lấy file đầu vào
    df = fetch_misa_data(file_path = file_path)
    print(f'\nSố dòng data ban đầu của Hà Nội: {len(df)}')
    
    
    # Xuất file nhân sự
    nhan_su = file_employees # file nay co UID , Tên nhân viên, Mã nhân viên
    
    #  --------------------------------------------------------------------
    #  ---------------------------XỬ LÝ DỮ LIỆU----------------------------

    # Xử lý ngày tháng & kỳ công nợ
    df['Ngày hạch toán'] = pd.to_datetime(df['Ngày hạch toán'], format = '%d/%m/%Y')
    
    
    # ----------------SET UP PHÒNG BAN-------------------
    new_conditions = [
            (df['NV bán hàng'].str.contains('Nguyễn Văn A', na=False)), # phòng marketing
            (df['NV bán hàng'].str.endswith(' TN', na=False)), # phòng Thái Nguyên
            (df['NV bán hàng'].str.endswith('.', na=False)),  # phòng Beauty HN
            (df['NV bán hàng'].str.endswith(' Bb', na=False)), # phòng Bán buôn HN
            (df['NV bán hàng'].str.contains('Nguyễn Thị Q', na=False)), # Quyền ĐK
            (df['NV bán hàng'].str.contains(' ABC', na=False)), # Đào KT
            (df['NV bán hàng'].str.endswith('-T', na=False)) # KD Tỉnh
            ]
    
    HAI_DUONG_CODE = ['Nguyễn Văn A', 'Trần Văn B', 'Trần Toàn M']
    THAI_NGUYEN_CODE = ['NGUYEN VĂN TN', 'CAO THI TN']

    choices = ['Phòng Marketing', 'VP Thái Nguyên', 'Beauty HN', 'Bán Buôn HN', 'Phòng KD3', 'Nội Bộ', 'Phòng KDT']
    # set choice cho từng điều kiện set bên trên
    # choices = ['Phòng Marketing', 'Beauty HN', 'Bán Buôn HN', 'Phòng KD3', 'Nội Bộ', 'Phòng KDT']
    # Choice phòng ban theo np.select()
    df['Phòng ban'] = np.select(new_conditions, choices, default='Khác')
    df.loc[df['NV bán hàng'].isin(HAI_DUONG_CODE), 'Phòng ban'] = 'VP Hải Dương'
    df.loc[df['NV bán hàng'].isin(THAI_NGUYEN_CODE) , 'Phòng ban'] = 'VP Thái Nguyên'

    # ----------------SET UP LẤY MÃ NHÂN VIÊN-------------------
    # Tạo mã UID để nối với UID của dataframe nhan_su bên trên 
    df['UID'] = df['NV bán hàng'].astype(str) + "_" + df['Phòng ban'].astype(str)
    
    # Nối để lấy `Mã nhân viên` ở df nhân sự, using merge left
    df = pd.merge(left = df, right=nhan_su[['Mã nhân viên', 'UID']], how='left', left_on= 'UID', right_on= 'UID' )


    # ----------------TRANSFORM DỮ LIỆU NGÀY THÁNG -------------------
    # DEF HÀM TÍNH CÔNG NỢ MỚI VER2
    def tinh_ky_cong_no_v2(dt, phong_ban): # Ở file Hà Nội sẽ so sánh theo phòng ban con trong file đó
        if pd.isnull(dt) or phong_ban not in config_hn:
            # Nếu phòng ban không có trong config (ví dụ: Nội Bộ, Marketing), mặc định lấy theo tháng của ngày hạch toán
            return dt.to_period("M") if pd.notnull(dt) else None
        
        periods = config_hn[phong_ban]
        for p in periods:
            start = pd.to_datetime(p['start_date'])
            end = pd.to_datetime(p['end_date']) if p['end_date'] else pd.to_datetime('2099-12-31')
            
            if dt <= start:
                return (pd.to_datetime(p['ky_cong_no']) - pd.DateOffset(months=1)).to_period("M")
            elif start < dt <= end:
                return pd.to_datetime(p['ky_cong_no']).to_period("M")
            elif dt > end and p == periods[-1]:
                return (pd.to_datetime(p['ky_cong_no']) + pd.DateOffset(months=1)).to_period("M")
        return dt.to_period("M")

    # Dùng apply vào thẳng data thì phải set row[col] tương ứng cột bên trong df để lấy giá trị của cột, ở đây lấy 2 giá trị của `Ngày hạch toán` và `Phòng ban`, set axis=1 để thao tác với dòng
    df['Kỳ_Period'] = df.apply(lambda row: tinh_ky_cong_no_v2(row['Ngày hạch toán'], row['Phòng ban']), axis=1)
    # row['Ngày hạch toán'] và row['Phòng ban'] truyền vào để so sánh các điều kiện cùng hàng
    
    # Tách thông tin ngày tháng
    df['Tháng chốt công nợ'] = df['Kỳ_Period'].dt.month
    df['Năm chốt công nợ'] = df['Kỳ_Period'].dt.year
    df['Kỳ công nợ'] = df['Kỳ_Period'].astype(str) # set về dạng str(YYYY-mm)
    df = df.drop(columns = ['Kỳ_Period']) # Xóa bỏ cột Kỳ_Preiod
    
    
    # -----------------ÉP KIỂU DỮ LIỆU -------------------
    # INTEGER
    df['Số lượng bán'] = pd.to_numeric(df['Số lượng bán'], errors='coerce').fillna(0).astype(int)
    df['Số lượng trả lại'] = pd.to_numeric(df['Số lượng trả lại'], errors='coerce').fillna(0).astype(int)
    
    # FLOAT
    col_to_float = ['Đơn giá', 'Doanh số bán', 'Chiết khấu', 'Giá trị trả lại', 'Thuế GTGT', 'Tổng tiền thanh toán']
    for col_base in col_to_float:
        df[col_base] = df[col_base].astype(float)

    # df = df.rename(columns= {'Tên nhân viên' : 'NV bán hàng'})
    

    # -----------------CÁC CỘT CẦN GIỮ LẠI-----------------
    cols_to_take = ['Ngày hạch toán', 'Số chứng từ', 'Mã nhóm VTHH', 'Tên nhóm VTHH',
                    'Diễn giải chung', 'Diễn giải', 'Mã khách hàng', 'Tên khách hàng',
                    'Mã hàng', 'Tên hàng', 'ĐVT', 'Số lượng bán', 'Đơn giá', 'Doanh số bán',
                    'Chiết khấu', 'Số lượng trả lại', 'Giá trị trả lại', 'Giá trị giảm giá',
                    'Thuế GTGT', 'Tổng tiền thanh toán', 'Mã nhân viên', 'NV bán hàng',
                    'Phòng ban', 'Tháng chốt công nợ', 'Năm chốt công nợ', 'Kỳ công nợ']
    # Lấy các cột cần lấy
    df = df[cols_to_take]

    # -----------------SET UP LOẠI KHÁCH HÀNG-----------------
    
    # Set up Đại lý & Beauty Salon ,RULE : Đại lý (_DL_), Beauty Salon (_BS_) , không mã để khác (thường là điều chỉnh kho)
    df['Loại khách hàng'] = np.where(df['Mã khách hàng'].str.contains("_DL_"), "Đại lý", np.where(df['Mã khách hàng'].str.contains("_BS_") , "Beauty Salon" , "Khác"))
    
    # Set up nhà phân phối
    NPP_CODE =  ['abc', 'xyz', 'mna',
                'sad']
    
    # Phân loại theo NPP
    df.loc[df['Mã khách hàng'].isin(NPP_CODE) , 'Loại khách hàng' ] = 'Nhà phân phối'
    
    
    # -----------------TÍNH TOÁN CÁC CỘT CẦN THIẾT-----------------
    df['Doanh thu ròng'] = df['Tổng tiền thanh toán'] - df['Thuế GTGT']
    df['Thực xuất'] = df['Số lượng bán'] - df['Số lượng trả lại']


    # -----------------SET UP LẠI TÊN NHÂN VIÊN (DỂ SAU CÙNG)-----------------
    # Setup riêng tên nhân viên cho Hà Nội
    df['NV bán hàng'] = df['NV bán hàng'].str.strip() # bỏ khoảng trắng đi
    # Tạo danh sách các hậu tố cần xóa
    suffixes = [' TN', ' Bb', '.', '-T', ' .']

    for s in suffixes:
        df['NV bán hàng'] = df['NV bán hàng'].str.removesuffix(s).str.strip()
    df['NV bán hàng'] = df['NV bán hàng'].replace('Tạ Thị ABC', 'Tạ Thị ABC (tk new)')

    return df.sort_values(by = 'Ngày hạch toán')





    