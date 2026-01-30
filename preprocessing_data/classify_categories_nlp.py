import json
import logging
from pathlib import Path
import sys

# --- KIỂM TRA THƯ VIỆN ---
try:
    from transformers import pipeline
    from tqdm import tqdm  # Thư viện tạo thanh hiển thị tiến độ
except ImportError:
    print("Error: Missing required libraries. Please run: pip install transformers torch tqdm")
    sys.exit(1)

# --- CẤU HÌNH ĐƯỜNG DẪN FILE ---
# Lấy đường dẫn của file script hiện tại
CURRENT_SCRIPT_DIR = Path(__file__).resolve().parent
# Lấy thư mục gốc (parent của folder data)
ROOT_DIR = CURRENT_SCRIPT_DIR.parent

# Định nghĩa file đầu vào và đầu ra
INPUT_FILE = ROOT_DIR / "category_analysis.json"
OUTPUT_FILE = ROOT_DIR / "analysis_results" / "classified_categories_nlp_result.json"

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CẤU HÌNH MODEL NLP ---
# Sử dụng model Zero-Shot Classification (nhẹ, chạy ổn trên CPU)
MODEL_NAME = "valhalla/distilbart-mnli-12-1"

# Định nghĩa danh sách các Nhóm Cha (Labels)
# Model sẽ tự động hiểu ngữ nghĩa và gán category con vào nhóm phù hợp nhất
CANDIDATE_LABELS = [
    "Food and Dining",             # Ẩm thực, nhà hàng
    "Health and Medical",          # Y tế, sức khỏe
    "Automotive and Transport",    # Xe cộ, vận tải
    "Retail and Shopping",         # Mua sắm, bán lẻ
    "Beauty and Wellness",         # Làm đẹp, spa, thể hình
    "Home Services and Construction", # Dịch vụ nhà cửa, xây dựng
    "Education and Community",     # Giáo dục, cộng đồng, tôn giáo
    "Entertainment and Travel",    # Giải trí, du lịch
    "Industry and Manufacturing",  # Công nghiệp, sản xuất
    "Financial and Legal Services" # Tài chính, luật pháp
]

def process_classification_nlp():
    """
    Hàm chính để thực hiện phân loại sử dụng AI Model.
    """
    logger.info("=" * 50)
    logger.info(f"STARTING NLP CLASSIFICATION (Zero-Shot)")
    logger.info(f"Model: {MODEL_NAME}")
    logger.info("=" * 50)

    # 1. Kiểm tra file đầu vào
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found at: {INPUT_FILE}")
        return

    try:
        # Đọc dữ liệu từ file JSON
        logger.info(f"Reading data from {INPUT_FILE}...")
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Lấy danh sách các category cần phân loại
        unique_list = data.get('unique_list', [])
        
        if not unique_list:
            logger.warning("No categories found in 'unique_list'. Exiting.")
            return

        logger.info(f"Found {len(unique_list)} items to classify.")

        # 2. Tải Model (Lần đầu chạy sẽ tải về máy, các lần sau dùng cache)
        logger.info("Loading NLP Pipeline...")
        classifier = pipeline("zero-shot-classification", model=MODEL_NAME)

        classified_results = []
        # Khởi tạo bộ đếm thống kê cho từng nhóm
        group_statistics = {label: 0 for label in CANDIDATE_LABELS}

        logger.info("Processing classification...")
        
        # 3. Vòng lặp phân loại từng category (Sử dụng tqdm để hiện thanh tiến độ)
        # Model sẽ so sánh category hiện tại với danh sách CANDIDATE_LABELS
        for category in tqdm(unique_list, desc="Classifying"):
            
            # Gọi model để dự đoán
            result = classifier(category, CANDIDATE_LABELS)
            
            # Lấy nhãn có điểm số cao nhất (top 1)
            best_label = result['labels'][0]
            confidence_score = result['scores'][0]

            # Lưu kết quả chi tiết
            classified_results.append({
                "original_category": category,
                "assigned_group": best_label,
                "confidence_score": round(confidence_score, 4)  # Làm tròn 4 số thập phân
            })
            
            # Cập nhật thống kê
            group_statistics[best_label] += 1

        # 4. Chuẩn bị dữ liệu đầu ra
        output_data = {
            "model_info": {
                "name": MODEL_NAME,
                "type": "Zero-Shot Classification"
            },
            "statistics": group_statistics,
            "details": classified_results
        }

        # 5. Ghi kết quả ra file JSON
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)

        logger.info("=" * 50)
        logger.info(f"COMPLETED. Results saved to: {OUTPUT_FILE}")
        logger.info("--- Group Statistics ---")
        for group, count in group_statistics.items():
            logger.info(f"{group}: {count}")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
        # In chi tiết lỗi để debug nếu cần
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    process_classification_nlp()