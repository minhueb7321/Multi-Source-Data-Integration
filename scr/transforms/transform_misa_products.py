import pandas as pd
import os
from scr.extract import fetch_misa_products


from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os
from scr.extract import fetch_misa_products

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

def process_single_file(province_name, config, folder_name):
    """Hàm xử lý cho từng file đơn lẻ"""
    file_name = config['file_name']
    path_file = os.path.join(folder_name, file_name)
    
    if os.path.exists(path_file):
        try:
            # Đọc file (Nên dùng engine='calamine' trong fetch_misa_products để nhanh hơn)
            df = fetch_misa_products(path_file)
            df['Phòng ban'] = province_name
            print(f"✅ Đã xử lý file MISA sản phẩm thành công: {province_name}")
            return df
        except Exception as e:
            print(f"❌ Lỗi khi đọc file {province_name}: {e}")
            return None
    else:
        print(f"⚠️ Không tìm thấy file: {file_name}")
        return None

def concatenate_all_products(folder_name):
    # Chuẩn bị danh sách tham số để map vào executor
    # Mỗi item là (tên_văn_phòng, cấu_hình)
    items = list(CONFIG_PROVINCES.items())
    
    # Sử dụng ThreadPoolExecutor để đọc song song
    # max_workers=8 là con số cân bằng cho 12 file
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Sử dụng list comprehension kết hợp executor.submit hoặc map
        # Ở đây dùng list comprehension để dễ truyền folder_name
        futures = [executor.submit(process_single_file, name, cfg, folder_name) for name, cfg in items]
        results = [f.result() for f in futures]
    
    # Lọc bỏ các kết quả None (do file không tồn tại hoặc lỗi)
    all_files = [df for df in results if df is not None]
    
    if all_files:
        all_df = pd.concat(all_files, ignore_index=True)
        print("-" * 30)
        print(f"Tổng số dòng: {len(all_df)} | Tổng số cột: {len(all_df.columns)}")
        return all_df
    
    print("⚠️ Không có dữ liệu để tổng hợp.")
    return pd.DataFrame()