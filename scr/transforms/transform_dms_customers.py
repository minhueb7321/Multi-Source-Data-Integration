import pandas as pd
import numpy as np
from scr.extract import fetch_dms_file_khach_hang_tiem_nang_chung


def tinh_toan_kh_kho_chung(file_name_kh, file_name_kh_chung, file_name_tn,file_name_tn_chung,path_dms_employees=None):

#  ---------------------------------------------------------------------------------------------
#  ---------------------------KHÁCH HÀNG + TIỀM NĂNG RIÊNG VÀ CHUNG-----------------------------
    # ĐỌC HẾT
    df_kh , df_tn, df_kh_chung, df_tn_chung = fetch_dms_file_khach_hang_tiem_nang_chung(file_path_kh=file_name_kh,
                                                                                        file_path_tn= file_name_tn,
                                                                                        file_path_kh_chung=file_name_kh_chung,
                                                                                        file_path_tn_chung = file_name_tn_chung
                                                                                        )

    # Xử lý data khách hàng
    df_kh = df_kh[['Mã khách hàng', 'Tên khách hàng', 'Người phụ trách', 'Trạng thái thông tin', 'Ngày chuyển đổi', 'Phòng ban']]
    # Lấy data từ các chi nhanh cần thiết
    values_to_take = ['VP Sài Gòn', 'VP Cần Thơ', 'VP Vinh', 'VP Hưng Yên','Beauty HN', 'VP Đắk Lắk', 'VP Hải Phòng', 'VP Đà Nẵng','VP Nam Định', 'VP Đồng Nai', 'VP Hải Dương', 'VP Nha Trang','Bán buôn HN', 'KD3', 'VP Thanh Hóa', 'Phòng KDT','VP Thái Nguyên' ]
    df_kh = df_kh[df_kh['Phòng ban'].isin(values_to_take)]
    # Thay đổi giá trị văn phòng
    map_value = {"VP Hưng Yên" : "VP Hải Dương"}
    df_kh['Phòng ban'] = df_kh['Phòng ban'].replace(map_value)
    # Chuyển cột ngày chuyển đổi sang timestamp
    df_kh['Ngày chuyển đổi'] = pd.to_datetime(df_kh['Ngày chuyển đổi'], format='%H:%M:%S %d/%m/%Y')


    df_kh_chung = df_kh_chung[['Mã khách hàng', 'Tên khách hàng', 'Trạng thái thông tin', 'Ngày chuyển đổi', 'Phòng ban']]
    df_kh_chung['Người phụ trách'] = 'Kho Chung'
    df_kh_chung = df_kh_chung[df_kh_chung['Phòng ban'].isin(values_to_take)]
    # Thay đổi giá trị văn phòng
    map_value = {"VP Hưng Yên" : "VP Hải Dương"}
    df_kh_chung['Phòng ban'] = df_kh_chung['Phòng ban'].replace(map_value)
    # Chuyển cột ngày chuyển đổi sang timestamp
    df_kh_chung['Ngày chuyển đổi'] = pd.to_datetime(df_kh_chung['Ngày chuyển đổi'], format='%H:%M:%S %d/%m/%Y')


    KH_ALL = pd.concat([df_kh_chung, df_kh], ignore_index=True)

    mapping_kho = {
    "Beauty HN": "Kho chung BS",
    "Bán buôn HN": "Kho chung BB",
    "Phòng KDT": "Kho chung KDT",
    "VP Cần Thơ": "Kho chung CT",
    "VP Hải Dương": "Kho chung HD",
    "VP Hải Phòng": "Kho chung HP",
    "VP Nam Định": "Kho chung ND",
    "VP Nha Trang": "Kho chung NT",
    "VP Sài Gòn": "Kho chung SG",
    "VP Thanh Hóa": "Kho chung TH",
    "VP Thái Nguyên": "Kho chung TN",
    "VP Vinh": "Kho chung V",
    "VP Đà Nẵng": "Kho chung DNG",
    "VP Đắk Lắk": "Kho chung DL",
    "VP Đồng Nai": "Kho chung DNI"
    }


    # PIVOT TÍNH TOÁN
    PIVOT_KH = pd.pivot_table(
        KH_ALL,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã khách hàng'],
        aggfunc={
            'Mã khách hàng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()

    PIVOT_KH_MASK  =PIVOT_KH['Người phụ trách'].eq("Kho Chung")
    PIVOT_KH.loc[PIVOT_KH_MASK, 'Người phụ trách'] = PIVOT_KH.loc[PIVOT_KH_MASK, "Phòng ban"].map(mapping_kho).fillna("Kho Chung")


#  -----------------------------------------------------------------
#  -----------------------TIỀM NĂNG---------------------------------

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



    # Lọc các cột cần lấy
    df_tn_chung = df_tn_chung[['Mã tiềm năng', 'Tên tiềm năng', 'Trạng thái', 'Ngày tạo', 'Phòng ban']]
    df_tn_chung['Người phụ trách'] = 'Kho Chung'
    # Lấy data từ các chi nhanh cần thiết
    values_to_take = ['VP Sài Gòn', 'VP Cần Thơ', 'VP Vinh', 'VP Hưng Yên','Beauty HN', 'VP Đắk Lắk', 'VP Hải Phòng', 'VP Đà Nẵng','VP Nam Định', 'VP Đồng Nai', 'VP Hải Dương', 'VP Nha Trang','Bán buôn HN', 'KD3', 'VP Thanh Hóa', 'Phòng KDT','VP Thái Nguyên' ]
    df_tn_chung = df_tn_chung[df_tn_chung['Phòng ban'].isin(values_to_take)]
    # Thay đổi giá trị văn phòng
    map_value = {"VP Hưng Yên" : "VP Hải Dương"}
    df_tn_chung['Phòng ban'] = df_tn_chung['Phòng ban'].replace(map_value)
    # Chuyển cột ngày chuyển đổi sang timestamp
    df_tn_chung['Ngày tạo'] = pd.to_datetime(df_tn_chung['Ngày tạo'], format='%H:%M:%S %d/%m/%Y')

    TN_ALL = pd.concat([df_tn_chung, df_tn], ignore_index=True)


#  -----------------------------------------------------------------
#  -----------------------TỔNG ALL----------------------------------

    # Đổi tên cột để concat
    TN_ALL.rename(columns={'Mã tiềm năng': 'Mã khách hàng', 'Tên tiềm năng' : 'Tên khách hàng', 'Ngày tạo' : 'Ngày chuyển đổi'}, inplace=True)
    KH_TN_ALL = pd.concat([TN_ALL, KH_ALL], ignore_index=True)

    # PIVOT TÍNH TOÁN
    PIVOT_ALL = pd.pivot_table(
        KH_TN_ALL,
        index=['Phòng ban', 'Người phụ trách'],
        values=['Mã khách hàng'],
        aggfunc={
            'Mã khách hàng': 'count',
        },
        margins=True,
        margins_name='TOTAL'
    ).reset_index()

    PIVOT_ALL_MASK  =PIVOT_ALL['Người phụ trách'].eq("Kho Chung")
    PIVOT_ALL.loc[PIVOT_ALL_MASK, 'Người phụ trách'] = PIVOT_ALL.loc[PIVOT_ALL_MASK, "Phòng ban"].map(mapping_kho).fillna("Kho Chung")

    df_dms_employees = pd.read_excel(path_dms_employees, usecols= ['Mã Nhân Viên', 'Phòng ban', 'Tên Nhân Viên'])
    df_dms_employees['Phòng ban'] = df_dms_employees['Phòng ban'].replace(map_value)
    df_dms_employees['UID'] = df_dms_employees['Tên Nhân Viên'].astype(str) + '_' + df_dms_employees['Phòng ban'].astype(str)
    
    dfs = [PIVOT_KH, PIVOT_ALL]
    for i in range(len(dfs)):
        dfs[i]['UID'] =  dfs[i]['Người phụ trách'].astype(str) + '_' + dfs[i]['Phòng ban'].astype(str)
        dfs[i] = pd.merge(left = dfs[i], right = df_dms_employees, how = 'left', right_on = 'UID', left_on = 'UID')
        dfs[i] = dfs[i][['Phòng ban_x', 'Mã Nhân Viên', 'Người phụ trách', 'Mã khách hàng']]
        dfs[i] = dfs[i].rename(columns={'Phòng ban_x': 'Phòng ban'})


    PIVOT_KH, PIVOT_ALL = dfs[0], dfs[1]
    return PIVOT_KH , PIVOT_ALL



from concurrent.futures import ThreadPoolExecutor

def lightning_transform(file_name_kh, file_name_kh_chung, file_name_tn, file_name_tn_chung):
    file_paths = [file_name_kh, file_name_tn, file_name_kh_chung, file_name_tn_chung]
    labels = ['Khách hàng', 'Tiềm năng', 'Khách hàng chung', 'Tiềm năng chung'] # gắn nhãn cho từng tệp file
    print(f"✅ Đang đọc song song 4 file khách hàng...")

    # Sử dụng ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as executor:
        # executor.map sẽ trả về kết quả theo đúng thứ tự file_paths truyền vào
        results = list(executor.map(pd.read_excel, file_paths))

    # Gán nhãn "Dạng khách hàng" cho từng DataFrame sau khi đọc xong
    for df, label in zip(results, labels):
        df['Dạng khách hàng'] = label

    print("✅ Đọc và xử lý thành công 4 file khách hàng trên DMS!")
    
    # Trả về đúng thứ tự như cũ để không làm hỏng code bên ngoài
    return results[0], results[1], results[2], results[3]