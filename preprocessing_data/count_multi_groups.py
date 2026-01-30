import json
from pathlib import Path
from collections import Counter

# --- CẤU HÌNH ĐƯỜNG DẪN ---
CURRENT_SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = CURRENT_SCRIPT_DIR.parent
INPUT_FILE = ROOT_DIR / "analysis_results" / "business_group_mapping_result.json"

def analyze_detailed_groups():
    print("=" * 60)
    print("PHÂN TÍCH CHI TIẾT SỐ LƯỢNG NHÓM NGÀNH")
    print("=" * 60)

    if not INPUT_FILE.exists():
        print(f"Lỗi: Không tìm thấy file dữ liệu tại {INPUT_FILE}")
        return

    try:
        print(f"Đang đọc dữ liệu...")
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Bộ đếm: Key = Số lượng nhóm, Value = Số lượng doanh nghiệp
        stats = Counter()
        total_multi_group_biz = 0
        
        # Duyệt qua từng doanh nghiệp để đếm
        for biz in data:
            groups = biz.get('new_groups', [])
            num_groups = len(groups)
            
            # Chỉ quan tâm các doanh nghiệp có từ 2 nhóm trở lên
            if num_groups >= 2:
                stats[num_groups] += 1
                total_multi_group_biz += 1

        if total_multi_group_biz == 0:
            print("Không tìm thấy doanh nghiệp nào có từ 2 nhóm ngành trở lên.")
            return

        # --- IN KẾT QUẢ RA MÀN HÌNH ---
        print(f"Tổng số doanh nghiệp đa ngành (>= 2 nhóm): {total_multi_group_biz}")
        print("-" * 60)
        print(f"{'SỐ LƯỢNG NHÓM':<20} | {'SỐ DOANH NGHIỆP':<20} | {'TỶ LỆ %':<10}")
        print("-" * 60)

        # Sắp xếp theo key (số nhóm: 2, 3, 4...) để in cho đẹp
        for num_groups in sorted(stats.keys()):
            count = stats[num_groups]
            # Tính phần trăm trong tổng số doanh nghiệp đa ngành
            percent = (count / total_multi_group_biz) * 100
            
            print(f"{num_groups:<20} | {count:<20} | {percent:.2f}%")

        print("-" * 60)
        print("Hoàn tất (Không lưu file).")

    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

if __name__ == "__main__":
    analyze_detailed_groups()