import pandas as pd
import os 
import openpyxl

def load_file_excel(df, file_path, custom_name):
    """
    df : DataFrame cần lưu
    file_path : Đường dẫn tới thư mục
    custom_name : Tên file muốn custom theo ý muốn
    """

    #  1.Check thư mục
    if not os.path.exists(file_path):
        os.makedirs(file_path)
        print(f"Đã tạo thư mục {file_path}")

    # 2. Kết hợp đường dẫn thư mục
    full_file_name = f"Data{custom_name}.xlsx"
    final_path = os.path.join(file_path, full_file_name)

    # 3. Xuất file
    df.to_excel(final_path, index=False)
    print(f"✅ Đã xuất file DMS thành công: {final_path}")
    

