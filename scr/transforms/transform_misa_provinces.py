import pandas as pd
import numpy as np
from scr.extract import fetch_misa_data


# Truyền thêm file nhân sự trên DMS để nối thẳng vào SCT
def process_misa_file_provinces(file_path, province_name, config_periods, df_employees:pd.DataFrame):

    """
    Truyền vào file_path + tên vùng + ngày chốt công nợ 
    """
    df = fetch_misa_data(file_path = file_path)
    print(f'Số dòng data ban đầu của {province_name}: {len(df)}')
    
    nhan_su = df_employees

    # ---------------------------XỬ LÝ DỮ LIỆU----------------------------
    # Set up tên phòng ban
    df['Phòng ban'] = province_name
    # đối với tỉnh phải set được tên phòng ban trước để nối được UID

    df['UID'] = df['NV bán hàng'].astype(str) + "_" + df['Phòng ban'].astype(str)

    df = pd.merge(left = df, right=nhan_su[['Mã nhân viên', 'UID']], how='left', left_on= 'UID', right_on= 'UID' )
    
    # Xử lý ngày tháng & kỳ công nợ
    df['Ngày hạch toán'] = pd.to_datetime(df['Ngày hạch toán'], format = '%d/%m/%Y')
    

    # ---------------------TRANSFORM DỮ LIỆU NGÀY THÁNG--------------------
    # DEF HÀM TÍNH CÔNG NỢ MỚI VER2
    def tinh_ky_cong_no_v2(dt, periods):
        if pd.isnull(dt)  : return None 
        
        # periods : tập con của info['periods'] truyền vào vòng lặp, list của dict
        for p in periods: # p o day la dict , dung p[] de get value
            start = pd.to_datetime(p['start_date'])
            end = pd.to_datetime(p['end_date']) if p['end_date'] else pd.to_datetime('2099-12-31')

            # Logic ghim start/end 
            
            # TH1: Trước start_date -> Kỳ = Kỳ hiện tại -1 tháng
            if dt <= start :
                return (pd.to_datetime(p['ky_cong_no']) - pd.DateOffset(months=1)).to_period("M")
            # TH2: Nằm trong khoảng [start, end] -> Lấy đúng kỳ trong dicts
            elif start < dt <= end:
                return pd.to_datetime(p['ky_cong_no']).to_period("M")
            # TH3: Sau end_date và dòng config cuối -> Kỳ = Kỳ hiện tại + 1 tháng
            elif dt > end and p == periods[-1]:
                return (pd.to_datetime(p['ky_cong_no']) + pd.DateOffset(months=1)).to_period("M") 
        
        return None
        
    # Áp dụng cú pháp logic mới
    df['Kỳ_Period'] = df['Ngày hạch toán'].apply(lambda x: tinh_ky_cong_no_v2(x, config_periods)) 

    
    # Tách thông tin ngày tháng
    df['Tháng chốt công nợ'] = df['Kỳ_Period'].dt.month
    df['Năm chốt công nợ'] = df['Kỳ_Period'].dt.year
    df['Kỳ công nợ'] = df['Kỳ_Period'].astype(str)
    df = df.drop(columns = 'Kỳ_Period')


    # -----------------ÉP KIỂU DỮ LIỆU -------------------
    # INTEGER
    df['Số lượng bán'] = pd.to_numeric(df['Số lượng bán'], errors='coerce').fillna(0).astype(int)
    df['Số lượng trả lại'] = pd.to_numeric(df['Số lượng trả lại'], errors='coerce').fillna(0).astype(int)
    
    # FLOAT
    col_to_float = ['Đơn giá', 'Doanh số bán', 'Chiết khấu', 'Giá trị trả lại', 'Thuế GTGT', 'Tổng tiền thanh toán']
    for col_base in col_to_float:
        df[col_base] = df[col_base].astype(float)
        
        
    # -----------------CÁC CỘT CẦN GIỮ LẠI-----------------

    cols_to_take = ['Ngày hạch toán', 'Số chứng từ', 'Mã nhóm VTHH', 'Tên nhóm VTHH',
                    'Diễn giải chung', 'Diễn giải', 'Mã khách hàng', 'Tên khách hàng',
                    'Mã hàng', 'Tên hàng', 'ĐVT', 'Số lượng bán', 'Đơn giá', 'Doanh số bán',
                    'Chiết khấu', 'Số lượng trả lại', 'Giá trị trả lại', 'Giá trị giảm giá',
                    'Thuế GTGT', 'Tổng tiền thanh toán', 'Mã nhân viên', 'NV bán hàng',
                    'Phòng ban', 'Tháng chốt công nợ', 'Năm chốt công nợ', 'Kỳ công nợ']
    df = df[cols_to_take]

    # -----------------SET UP LOẠI KHÁCH HÀNG-----------------

    # Thêm mã của NPP
    NPP_CODE =  ['HN_DL_MYPHAMHONGNHUNG', 'HN_DL_THANGMUNG', 'HN_DL_MYPHAMTUANTHAO',
                'HN_DL_KDT_DL_DAILYQUYNHANH', 'HN_DL_NGOCMAI', 'HN_DL_TUANLAI',
                'HN_DL_THUYHOANG', 'HN_DL_TUANTUYET']
    
    # Phân loại khách hàng theo _DL_ và _BS_
    df['Loại khách hàng'] = np.where(df['Mã khách hàng'].str.contains("_DL_"), "Đại lý", np.where(df['Mã khách hàng'].str.contains("_BS_") , "Beauty Salon" , "Khác"))

    # Phân loại theo NPP
    df.loc[df['Mã khách hàng'].isin(NPP_CODE) , 'Loại khách hàng' ] = 'Nhà phân phối'
    
    
    # -----------------TÍNH TOÁN CÁC CỘT CẦN THIẾT----------------- 

    df['Doanh thu ròng'] = df['Tổng tiền thanh toán'] - df['Thuế GTGT']
    df['Thực xuất'] = df['Số lượng bán'] - df['Số lượng trả lại']
    
    # -----------------SET UP LẠI TÊN NHÂN VIÊN--------------------
    df['NV bán hàng'] = df['NV bán hàng'].str.title()

    return df.sort_values(by = 'Ngày hạch toán')



