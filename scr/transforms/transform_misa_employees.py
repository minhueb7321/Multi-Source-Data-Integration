import pandas as pd
import numpy as np
import os
from scr.extract import fetch_misa_employees


CONFIG_PROVINCES = {
    'Hà Nội': {'file_name': 'ha noi.xls'},
    'VP Cần Thơ': {'file_name': 'can tho.xls'},
    'VP Đà Nẵng': {'file_name': 'da nang.xls'},
    'VP Đăk Lăk': {'file_name': 'dak lak.xls'},
    'VP Đồng Nai': {'file_name': 'dong nai.xls'},
    'VP Hải Dương': {'file_name': 'hai duong.xls'},
    'VP Hải Phòng': {'file_name': 'hai phong.xls'},
    'VP Nam Định': {'file_name': 'nam dinh.xls'},
    'VP Nha Trang': {'file_name': 'nha trang.xls'},
    'VP Sài Gòn': {'file_name': 'sai gon.xls'},
    'VP Thanh Hóa': {'file_name': 'thanh hoa.xls'},
    'VP Vinh': {'file_name': 'vinh.xls'}
}

def concatnate_all_employees(folder_name):
    all_files = []
    
    # Duyệt theo Dictionary để kiểm soát đúng file và đúng tên Phòng ban
    for province_name, config in CONFIG_PROVINCES.items():
        file_name = config['file_name']
        path_file = os.path.join(folder_name, file_name)
        
        if os.path.exists(path_file):
            df = fetch_misa_employees(path_file)

            if province_name == 'Hà Nội':
                # --- LOGIC RIÊNG CHO HÀ NỘI ---
                new_conditions = [
                    (df['Tên nhân viên'].str.contains('Nguyễn Tuấn Thành', na=False)),
                    (df['Tên nhân viên'].str.endswith(' TN', na=False)),
                    (df['Tên nhân viên'].str.endswith('.', na=False)),
                    (df['Tên nhân viên'].str.endswith(' Bb', na=False)),
                    (df['Tên nhân viên'].str.contains('Nguyễn Quang Quyền', na=False)),
                    (df['Tên nhân viên'].str.contains('Tạ Thị Đào', na=False)),
                    (df['Tên nhân viên'].str.endswith('-T', na=False))
                ]
                choices = [
                    'Phòng Marketing', 'VP Thái Nguyên', 'Beauty HN', 
                    'Bán Buôn HN', 'Phòng KD3', 'Nội Bộ', 'Phòng KDT'
                ]
                # Nếu không khớp các điều kiện trên, mặc định để là 'Hà Nội'
                df['Phòng ban'] = np.select(new_conditions, choices, default='Hà Nội')
                df['UID'] = df['Tên nhân viên'].astype(str) + "_" + df['Phòng ban'].astype(str)
            else:
                df['Phòng ban'] = province_name
                df['UID'] = df['Tên nhân viên'].astype(str) + "_" + df['Phòng ban'].astype(str)
            all_files.append(df)
            print(f"✅ Đã xử lý file MISA nhân sự thành công: {province_name}")
        else:
            print(f"❌ Thiếu file: {file_name} của {province_name}")

    if all_files:
        final_df = pd.concat(all_files, ignore_index=True)
        print(f"Tổng số dòng: {len(final_df)} | Tổng số cột: {len(final_df.columns)}")
        return final_df
    else:
        return None


