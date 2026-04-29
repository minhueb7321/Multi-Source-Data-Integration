import logging
import os
import sys

def quick_log(name):
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        current_file_path = os.path.abspath(__file__) 
        # 2. Lấy thư mục chứa nó (folder 'source')
        source_dir = os.path.dirname(current_file_path)
        # 3. Nhảy ra ngoài 1 cấp để ra thư mục 'root'
        root_dir = os.path.dirname(source_dir)
        # 4. Tạo đường dẫn file log nằm ở root
        log_path = os.path.join(root_dir, 'system.log')
        # ----------------------------------------

        formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')

        # Ghi vào file ở Root
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # In ra màn hình
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        # Chặn log bị lặp nếu có dùng thư mục cha
        logger.propagate = False
        
    return logger
