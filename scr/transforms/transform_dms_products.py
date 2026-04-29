from scr.extract import fetch_dms_file_products

def light_trasform_dms_products(file_path):
    df = fetch_dms_file_products(file_path)
    return df