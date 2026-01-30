import json
import logging
from pathlib import Path
from tqdm import tqdm

# --- CẤU HÌNH ĐƯỜNG DẪN ---
CURRENT_SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_SCRIPT_DIR.parent

# File đầu vào 1: Kết quả phân loại NLP (Bảng tra cứu)
MAPPING_FILE = ROOT_DIR / "analysis_results" / "classified_categories_nlp_result.json"
# File đầu vào 2: Dữ liệu doanh nghiệp gốc
META_FILE = ROOT_DIR / "data" / "meta-Washington.json"
# File đầu ra: Kết quả mapping cuối cùng
OUTPUT_FILE = ROOT_DIR / "analysis_results" / "business_group_mapping_result.json"

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_mapping_dict(mapping_file_path):
    """
    Đọc file kết quả NLP và chuyển thành Dictionary để tra cứu nhanh.
    Output: { "Sushi restaurant": "Food and Dining", "Plumber": "Home Services", ... }
    """
    if not mapping_file_path.exists():
        logger.error(f"Mapping file not found: {mapping_file_path}")
        return None

    try:
        with open(mapping_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Biến đổi list details thành dictionary
        # data['details'] là list các object: [{"original_category": "A", "assigned_group": "B"}, ...]
        mapping_dict = {}
        for item in data.get('details', []):
            original = item.get('original_category') or item.get('category') # Handle cả format cũ/mới
            group = item.get('assigned_group') or item.get('group')
            
            if original and group:
                mapping_dict[original] = group
                
        logger.info(f"Loaded {len(mapping_dict)} mapping rules.")
        return mapping_dict
    except Exception as e:
        logger.error(f"Error loading mapping file: {e}")
        return None

def process_business_mapping():
    logger.info("=" * 50)
    logger.info("STARTING BUSINESS GROUP MAPPING")
    logger.info("=" * 50)

    # 1. Load bảng mapping
    category_map = load_mapping_dict(MAPPING_FILE)
    if not category_map:
        return

    # 2. Xử lý file meta
    if not META_FILE.exists():
        logger.error(f"Meta file not found: {META_FILE}")
        return

    mapped_businesses = []
    
    try:
        logger.info(f"Reading and processing {META_FILE}...")
        
        # Đọc file meta (dạng JSON Lines - mỗi dòng là 1 object JSON)
        # Dùng chunksize để đọc nếu file quá lớn, ở đây đọc từng dòng
        count = 0
        with open(META_FILE, 'r', encoding='utf-8') as f:
            # Đếm tổng số dòng để hiện thanh progress bar (tùy chọn, có thể bỏ qua bước đếm này nếu file quá lớn)
            # lines = f.readlines() 
            # total_lines = len(lines)
            # f.seek(0) # Reset con trỏ về đầu file
            
            for line in tqdm(f, desc="Mapping Businesses"):
                try:
                    biz_data = json.loads(line)
                    
                    # Lấy thông tin cần thiết
                    biz_name = biz_data.get('name')
                    original_cats = biz_data.get('category') # Đây là một list: ['Cafe', 'Bookstore']
                    
                    if original_cats and isinstance(original_cats, list):
                        # --- LOGIC MAPPING QUAN TRỌNG ---
                        new_groups_set = set() # Dùng set để tự động loại bỏ trùng lặp
                        
                        for cat in original_cats:
                            cat_clean = str(cat).strip()
                            # Tra cứu trong từ điển mapping
                            found_group = category_map.get(cat_clean)
                            
                            if found_group:
                                new_groups_set.add(found_group)
                            else:
                                # Nếu category này chưa có trong file phân loại NLP (hiếm gặp nếu file NLP chạy từ list unique đủ)
                                new_groups_set.add("Uncategorized")

                        # Chuyển set về list để lưu vào JSON
                        new_groups_list = sorted(list(new_groups_set))
                        
                        # Chỉ lưu nếu có dữ liệu
                        mapped_businesses.append({
                            "name": biz_name,
                            "original_category": original_cats,
                            "new_groups": new_groups_list
                        })
                    
                    count += 1
                except json.JSONDecodeError:
                    continue

        # 3. Lưu kết quả ra file
        logger.info(f"Processed {count} businesses. Saving result...")
        
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapped_businesses, f, ensure_ascii=False, indent=4)
            
        logger.info(f"Successfully saved mapping to: {OUTPUT_FILE}")
        
        # In thử vài ví dụ
        if len(mapped_businesses) > 0:
            logger.info("--- Sample Results ---")
            for i in range(min(3, len(mapped_businesses))):
                logger.info(f"Biz: {mapped_businesses[i]['name']}")
                logger.info(f" -> Old: {mapped_businesses[i]['original_category']}")
                logger.info(f" -> New: {mapped_businesses[i]['new_groups']}")

    except Exception as e:
        logger.error(f"Error processing businesses: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    process_business_mapping()