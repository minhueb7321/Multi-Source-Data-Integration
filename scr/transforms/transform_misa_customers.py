import pandas as pd
import numpy as np
import os
from scr.extract import fetch_misa_customers

CONFIG_PROVINCES = {
    'Hà Nội': {'file_name': 'ha noi.xlsx'},
    'VP Cần Thơ': {'file_name': 'can tho.xlsx'},
    'VP Đà Nẵng': {'file_name': 'da nang.xlsx'},
    'VP Đăk Lăk': {'file_name': 'dak lak.xlsx'},
    'VP Đồng Nai': {'file_name': 'dong nai.xlsx'},
    'VP Hải Dương': {'file_name': 'hai duong.xlsx'},
    'VP Hải Phòng': {'file_name': 'hai phong.xlsx'},
    'VP Nam Định': {'file_name': 'nam dinh.xlsx'},
    'VP Nha Trang': {'file_name': 'nha trang.xlsx'},
    'VP Sài Gòn': {'file_name': 'sai gon.xlsx'},
    'VP Thanh Hóa': {'file_name': 'thanh hoa.xlsx'},
    'VP Vinh': {'file_name': 'vinh.xlsx'}
}

def concatenate_all_customers(folder_name):
    all_files = []
    
    # Duyệt theo Dictionary để kiểm soát đúng file và đúng tên Phòng ban
    for province_name, config in CONFIG_PROVINCES.items():
        file_name = config['file_name']
        path_file = os.path.join(folder_name, file_name)
        
        if os.path.exists(path_file):
            df = fetch_misa_customers(path_file)
            df['Phòng ban'] = province_name
        all_files.append(df)
        print(f"✅ Đã xử lý file MISA khách hàng thành công: {province_name}")
    if all_files:
        all_df = pd.concat(all_files, ignore_index=True)
        print(f"Tổng số dòng: {len(all_df)} | Tổng số cột: {len(all_df.columns)}")
        return all_df