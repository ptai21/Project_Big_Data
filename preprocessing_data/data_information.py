import pandas as pd
import glob
import os
import re
import time
from pathlib import Path
import logging
import json
from collections import Counter

# --- CẤU HÌNH ĐƯỜNG DẪN ---
# Sử dụng Path để tự động xử lý đường dẫn trên Linux/Windows
DATA_DIR = Path.home() / "bigdata" / "data"
# Đảm bảo đường dẫn tới split_reviews chính xác theo ảnh explorer của bạn
REVIEWS_DIR_PATH = str(DATA_DIR / "split_reviews" / "review-part-*.json")
META_FILE_PATH = DATA_DIR / "meta-Washington.json"

# --- THIẾT LẬP LOGGING TỰ ĐỘNG ---
# File log sẽ được tạo ngay trong thư mục data
LOG_FILE = DATA_DIR / "data_information.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'), # Ghi vào file
        logging.StreamHandler() # Hiển thị ra màn hình terminal
    ]
)
logger = logging.getLogger(__name__)

def analyze_reviews_dataset():
    """Phân tích các file reviews đã được chia nhỏ (128MB/file)"""
    logger.info("=" * 50)
    logger.info("STARTING REVIEWS DATA ANALYSIS")
    logger.info("=" * 50)

    start_time = time.time()
    
    review_files = sorted(glob.glob(REVIEWS_DIR_PATH))
    if not review_files:
        logger.error(f"No review files found in: {REVIEWS_DIR_PATH}")
        return

    all_pics_keys = set()
    unique_user_ids = set()
    total_records = 0
    
    for file_path in review_files:
        file_name = os.path.basename(file_path)
        # Sử dụng logger thay cho print để tự động lưu log
        logger.info(f"Processing {file_name}...")
        
        try:
            # Đọc dữ liệu review [cite: 1, 2]
            df_chunk = pd.read_json(file_path, lines=True)
            total_records += len(df_chunk)
            
            # 1. Kiểm tra cấu trúc 'pics' (list tập hợp nhiều url) 
            if 'pics' in df_chunk.columns:
                df_chunk['pics'].dropna().apply(
                    lambda x: [all_pics_keys.update(d.keys()) for d in x if isinstance(d, dict)]
                )
            
            # 2. Thu thập user_id (kiểu longint) 
            if 'user_id' in df_chunk.columns:
                unique_user_ids.update(df_chunk['user_id'].astype(str).unique())
        except Exception as e:
            logger.error(f"Error processing {file_name}: {e}")
            
    end_time = time.time()
    logger.info(f"Analysis completed in {end_time - start_time:.2f} seconds.")
    logger.info(f"Total records scanned: {total_records}")
    logger.info(f"Keys found in 'pics': {all_pics_keys}")
    logger.info(f"Total unique users (user_id): {len(unique_user_ids)}")

def analyze_meta_dataset():
    """Phân tích file meta-Washington.json"""
    logger.info("=" * 50)
    logger.info("STARTING META DATA ANALYSIS")
    logger.info("=" * 50)

    if not os.path.exists(META_FILE_PATH):
        logger.error(f"File not found: {META_FILE_PATH}")
        return

    try:
        # Đọc dữ liệu meta [cite: 3, 4]
        df_meta = pd.read_json(META_FILE_PATH, lines=True)
        
        # 1. Thống kê 'category' (list) 
        all_categories = []
        df_meta['category'].dropna().apply(lambda x: all_categories.extend(x))
        logger.info(f"1. Total unique categories: {len(set(all_categories))}")
        
        # 2. Giá trị 'price' ($$, $$$) 
        logger.info(f"2. Unique price values: {df_meta['price'].unique()}")
        
        # 3. Kiểm tra 'hours' (list of lists) 
        non_std_hours = df_meta['hours'].dropna().apply(
            lambda x: any(not isinstance(item, list) for item in x)
        ).sum()
        logger.info(f"3. Rows with non-standard 'hours' structure: {non_std_hours}")
        
        # 4. Kiểm tra 'MISC' (dictionary) 
        all_misc_keys = set()
        df_meta['MISC'].dropna().apply(lambda x: all_misc_keys.update(x.keys()))
        logger.info(f"4. Keys found in 'MISC' field: {all_misc_keys}")
        
        # 5. Kiểm tra 'state' và 'permanently closed' 
        perm_closed_count = df_meta['state'].str.contains('permanently closed', na=False, case=False).sum()
        logger.info(f"5. Total 'permanently closed' businesses: {perm_closed_count}")
        
        # 6. Trích xuất Thành phố
        def get_city(address):
            if not address: return None
            # 1. Regex tìm phần trước ", WA"
            match = re.search(r'([^,]+),\s*WA\s*\d+', address)
            if match:
                city_candidate = match.group(1).strip()
                # 2. Loại bỏ nhiễu: Nếu tên thành phố có chứa chữ số, khả năng cao là lấy nhầm số nhà
                if any(char.isdigit() for char in city_candidate):
                    # Cố gắng lấy phần chữ cuối cùng (thường là tên thành phố thật)
                    # Ví dụ: "123 Main St Kent" -> lấy "Kent"
                    return city_candidate.split()[-1] 
                return city_candidate
            return None

        # Trích xuất thành phố
        df_meta['city'] = df_meta['address'].apply(get_city)

        # Tính toán tần suất xuất hiện của từng thành phố
        city_counts = df_meta['city'].value_counts()

        logger.info(f"6. Total cities found: {len(city_counts)}")
        logger.info(f"Top 5 cities:\n{city_counts.head(5)}")

        # Lọc các địa danh chỉ xuất hiện ít (dưới 3 lần)
        rare_cities = city_counts[city_counts < 3]
        logger.info(f"Number of rare locations (count < 3): {len(rare_cities)}")

        # In ra 20 địa danh hiếm gặp nhất để xem có phải lỗi trích xuất không
        if len(rare_cities) > 0:
            logger.info(f"Sample rare locations (Possible noise):\n{rare_cities.tail(20)}")

        # --- PHẦN BỔ SUNG: LƯU RA FILE JSON ---
        try:
            # Chuyển đổi Series của Pandas sang Dictionary
            # city_counts có index là tên thành phố, value là số lượng
            output_data = city_counts.to_dict()
            
            file_name = 'city_statistics.json'
            
            with open(file_name, 'w', encoding='utf-8') as f:
                # ensure_ascii=False: Để hiển thị đúng ký tự Unicode (nếu có) thay vì mã \uXXXX
                # indent=4: Để format JSON đẹp, dễ đọc
                json.dump(output_data, f, ensure_ascii=False, indent=4)
                
            logger.info(f"Successfully saved city statistics to {file_name}")   
        except Exception as e:
            logger.error(f"Failed to save JSON file: {e}")

    except Exception as e:
        logger.error(f"Error processing meta file: {e}")

def deep_analyze_misc_field():
    """Phân tích chi tiết các giá trị bên trong cột MISC và lưu ra JSON"""
    logger.info("=" * 50)
    logger.info("STARTING MISC FIELD DEEP DIVE")
    logger.info("=" * 50)

    if not os.path.exists(META_FILE_PATH):
        logger.error(f"File not found: {META_FILE_PATH}")
        return

    try:
        # Đọc dữ liệu, chỉ cần cột MISC để tiết kiệm RAM
        logger.info("Reading meta file (MISC column only)...")
        # Dùng chunksize nếu file quá lớn, nhưng ở đây ta đọc hết nếu RAM đủ
        df = pd.read_json(META_FILE_PATH, lines=True)
        
        # Dictionary để lưu trữ các giá trị unique cho từng key
        # Cấu trúc: { 'Service options': set('Delivery', 'Takeout'...), 'Accessibility': set(...) }
        misc_details = {}

        logger.info("Processing MISC data...")
        
        # Lọc bỏ các dòng không có MISC
        misc_series = df['MISC'].dropna()

        count = 0
        for misc_dict in misc_series:
            count += 1
            if not isinstance(misc_dict, dict):
                continue
            
            for key, value in misc_dict.items():
                # Khởi tạo set nếu key chưa tồn tại
                if key not in misc_details:
                    misc_details[key] = set()
                
                # Xử lý value: Value có thể là list hoặc string/bool/int
                if isinstance(value, list):
                    # Nếu là list, thêm từng phần tử vào set
                    for item in value:
                        misc_details[key].add(str(item).strip())
                else:
                    # Nếu là giá trị đơn, thêm trực tiếp
                    misc_details[key].add(str(value).strip())

        logger.info(f"Processed {count} rows containing MISC data.")

        # --- CHUẨN BỊ DỮ LIỆU ĐỂ LƯU JSON ---
        output_data = {}
        
        logger.info("Aggregating results...")
        for key, val_set in misc_details.items():
            # Chuyển set thành list và sort để dễ nhìn
            unique_values = sorted(list(val_set))
            
            output_data[key] = {
                "unique_count": len(unique_values), # Số lượng giá trị unique
                "values": unique_values             # Danh sách chi tiết
            }
            
            logger.info(f"Key: '{key}' - Found {len(unique_values)} unique values.")

        # --- LƯU FILE ---
        output_filename = 'misc_deep_analysis.json'
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
            
        logger.info(f"Successfully saved detailed MISC analysis to {output_filename}")

    except Exception as e:
        logger.error(f"Error analyzing MISC field: {e}")
        import traceback
        traceback.print_exc()

def analyze_category_field():
    """Phân tích chi tiết cột category và lưu ra JSON"""
    logger.info("=" * 50)
    logger.info("STARTING CATEGORY FIELD ANALYSIS")
    logger.info("=" * 50)

    if not os.path.exists(META_FILE_PATH):
        logger.error(f"File not found: {META_FILE_PATH}")
        return

    try:
        # Đọc dữ liệu, chỉ cần cột category
        logger.info("Reading meta file (category column only)...")
        df = pd.read_json(META_FILE_PATH, lines=True)
        
        # Biến đếm tần suất xuất hiện của từng category
        category_counter = Counter()
        
        logger.info("Processing Category data...")
        
        # Lọc bỏ các dòng null
        cat_series = df['category'].dropna()

        count_rows = 0
        for cat_list in cat_series:
            count_rows += 1
            if isinstance(cat_list, list):
                # Nếu là list, cập nhật từng phần tử vào bộ đếm
                # strip() để xóa khoảng trắng thừa nếu có
                clean_cats = [str(c).strip() for c in cat_list]
                category_counter.update(clean_cats)
            else:
                # Trường hợp dữ liệu lỗi không phải list (hiếm gặp nhưng nên handle)
                category_counter.update([str(cat_list).strip()])

        logger.info(f"Processed {count_rows} rows containing category data.")

        # --- CHUẨN BỊ DỮ LIỆU ĐỂ LƯU JSON ---
        # Sắp xếp theo số lượng giảm dần (phổ biến nhất lên đầu)
        sorted_categories = dict(category_counter.most_common())
        
        output_data = {
            "summary": {
                "total_unique_categories": len(category_counter),
                "total_category_occurrences": sum(category_counter.values())
            },
            # Danh sách chi tiết kèm số lượng (Frequency)
            "details": sorted_categories,
            # Danh sách chỉ chứa tên (nếu bạn chỉ cần list thuần túy)
            "unique_list": sorted(list(category_counter.keys()))
        }
        
        logger.info(f"Found {len(category_counter)} unique categories.")
        logger.info(f"Top 5 categories: {list(sorted_categories.items())[:5]}")

        # --- LƯU FILE ---
        output_filename = 'category_analysis.json'
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)
            
        logger.info(f"Successfully saved category analysis to {output_filename}")

    except Exception as e:
        logger.error(f"Error analyzing category field: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # analyze_reviews_dataset()
    # analyze_meta_dataset()
    # deep_analyze_misc_field()
    analyze_category_field()