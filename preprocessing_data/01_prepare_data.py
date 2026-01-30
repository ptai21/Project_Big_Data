import os
import subprocess
import logging
from pathlib import Path

# ================= CONFIGURATION =================
DATA_DIR = Path.home() / "bigdata" / "data"
SPLITS_DIR = DATA_DIR / "split_reviews"
INPUT_FILE = DATA_DIR / "review-Washington.json"

# Cấu hình split: 128MB mỗi file (HDFS Block Size optimal)
# '128M' là tham số cho lệnh split
CHUNK_SIZE = "128M" 

# Setup Logging
LOG_FILE = DATA_DIR / "reviews_splitting.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_cmd(cmd):
    """Chạy lệnh shell và log lại kết quả."""
    logger.info(f"Executing: {cmd}")
    try:
        # check=True sẽ raise Exception nếu lệnh lỗi
        subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {cmd}")
        logger.error(f"Error details: {e.stderr.decode().strip()}")
        return False

def main():
    logger.info("=" * 50)
    logger.info("START PROCESS: SPLIT DATA BY SIZE (128MB)")
    logger.info("=" * 50)

    # 1. Kiểm tra file đầu vào
    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    # 2. Tạo thư mục output
    if not SPLITS_DIR.exists():
        os.makedirs(SPLITS_DIR)
        logger.info(f"Created directory: {SPLITS_DIR}")
    else:
        logger.warning(f"Directory exists (cleaning old files recommended): {SPLITS_DIR}")
        # Optional: Xóa file cũ nếu cần
        # run_cmd(f"rm -f {SPLITS_DIR}/*")

    # 3. Thực hiện Split
    # -C 128M: Chia theo size tối đa 128MB nhưng giữ nguyên vẹn dòng (line integrity)
    # -d: Sử dụng hậu tố số (00, 01...) thay vì chữ (aa, ab...)
    # --additional-suffix=.json: Thêm đuôi .json luôn, không cần rename sau này
    output_prefix = SPLITS_DIR / "review-part-"
    
    logger.info(f"Splitting file: {INPUT_FILE}")
    logger.info(f"Chunk strategy: ~{CHUNK_SIZE} per file (Line boundaries preserved)")
    
    cmd_split = f"split -C {CHUNK_SIZE} -d --additional-suffix=.json {INPUT_FILE} {output_prefix}"
    
    if run_cmd(cmd_split):
        logger.info("Split command executed successfully.")
    else:
        logger.error("Failed to split files. Exiting.")
        return

    # 4. Kiểm tra kết quả
    files = list(SPLITS_DIR.glob("review-part-*.json"))
    logger.info(f"Total files created: {len(files)}")
    
    # In ra 5 file đầu tiên để kiểm tra
    logger.info("Sample output files:")
    for f in files[:5]:
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f" - {f.name}: {size_mb:.2f} MB")

    logger.info("=" * 50)
    logger.info("DONE SPLITTING!")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()