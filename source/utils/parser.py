import re
import json
from uszipcode import SearchEngine


def extract_location_from_address(address_raw):
    """
    Trích xuất thông tin địa lý từ address sử dụng thư viện uszipcode.
    
    Output: Dictionary {'city': str, 'county': str, 'state': str}
    """
    # 1. Nếu address null -> trả về dict rỗng để PySpark xử lý
    if not address_raw:
        return {"city": None, "county": None, "state": None}

    try:
        # Regex: Tìm 5 chữ số ở cuối cùng của chuỗi (\d{5})$
        match = re.search(r'(\d{5})$', address_raw.strip())
        
        if match:
            zip_code = match.group(1)
            
            # [THAY ĐỔI] Sử dụng SearchEngine thay vì tra dictionary thủ công
            search = SearchEngine()
            zipcode_info = search.by_zipcode(zip_code)
            
            # Kiểm tra xem có tìm thấy dữ liệu không
            if zipcode_info and zipcode_info.zipcode:
                return {
                    "city": zipcode_info.major_city, # Lấy City
                    "county": zipcode_info.county,   # Lấy County
                    "state": zipcode_info.state      # Lấy State (nếu cần)
                }
                
    except Exception:
        pass
    
    # Fallback: Trả về None nếu không tìm thấy
    return {"city": None, "county": None, "state": None}


# --- 2. Xử lý Giờ mở cửa ---
def parse_hours(hours_array):
    """
    Input: [['Monday', '8AM–5PM'], ['Tuesday', 'Closed']]
    Output: {'Monday': '08:00-17:00', 'Tuesday': 'Closed'}
    """
    if not hours_array:
        return None
    
    result = {}
    try:
        for item in hours_array:
            if item and len(item) == 2:
                day = str(item[0]).strip() 
                time_range = str(item[1]).strip()
                result[day] = time_range
    except Exception:
        return None
    return result

# --- 3. Clean Text Review ---
def clean_text(text):
    if not text:
        return ""
    text = text.replace('\x00', '')
    # Xóa xuống dòng, tab
    text = text.replace('\n', ' ').replace('\r', '').replace('\t', ' ')
    return text.strip()






