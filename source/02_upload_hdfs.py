import subprocess
import time
import logging
from pathlib import Path

# ================= CONFIGURATION =================
DATA_DIR = Path.home() / "bigdata" / "data"
SPLITS_DIR = DATA_DIR / "split_reviews"
HDFS_BASE = "/user/bigdata/google-reviews"

# Setup Logging
LOG_FILE = DATA_DIR / "upload_dataset_into_datalake.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_cmd(cmd, show_output=True):
    """Chạy lệnh shell và trả về kết quả."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Command failed: {cmd}")
            logger.error(f"Stderr: {result.stderr.strip()}")
            return False, result.stderr
        
        if show_output and result.stdout.strip():
            logger.info(f"Stdout: {result.stdout.strip()}")
        return True, result.stdout
    except Exception as e:
        logger.error(f"Exception executing {cmd}: {str(e)}")
        return False, str(e)

def check_hdfs():
    logger.info("=" * 50)
    logger.info("CHECKING HDFS STATUS")
    
    success, _ = run_cmd("hdfs dfsadmin -report | head -10", show_output=False)
    if not success:
        logger.critical("HDFS is NOT running or accessible! Please start HDFS.")
        return False
    logger.info("HDFS is running OK.")
    return True

def create_hdfs_dirs():
    logger.info("=" * 50)
    logger.info("CREATING HDFS DIRECTORIES")
    
    dirs = [
        f"{HDFS_BASE}/raw/meta",
        f"{HDFS_BASE}/raw/reviews",
    ]
    
    for d in dirs:
        logger.info(f"Ensuring directory exists: {d}")
        run_cmd(f"hdfs dfs -mkdir -p {d}", show_output=False)

def upload_metadata():
    logger.info("=" * 50)
    logger.info("UPLOADING METADATA")
    
    meta_file = DATA_DIR / "meta-Washington.json"
    if meta_file.exists():
        start = time.time()
        logger.info(f"Uploading {meta_file.name}...")
        
        # -f: Overwrite nếu file đã tồn tại
        success, _ = run_cmd(f"hdfs dfs -put -f {meta_file} {HDFS_BASE}/raw/meta/", show_output=False)
        
        elapsed = time.time() - start
        if success:
            logger.info(f"Uploaded successfully: {meta_file.name} ({elapsed:.2f}s)")
        else:
            logger.error(f"Failed to upload {meta_file.name}")
    else:
        logger.error(f"Metadata file not found: {meta_file}")

def upload_reviews():
    logger.info("=" * 50)
    logger.info("UPLOADING REVIEW BATCHES")
    
    # Tìm tất cả các file review-part-*.json
    batch_files = sorted(SPLITS_DIR.glob("review-part-*.json"))
    
    if not batch_files:
        logger.warning(f"No batch files found in {SPLITS_DIR}. Did you run script 01?")
        return
    
    total = len(batch_files)
    logger.info(f"Found {total} batch files to upload.")
    
    start_total = time.time()
    success_count = 0
    
    for i, batch_file in enumerate(batch_files, 1):
        start = time.time()
        # Upload từng file
        success, _ = run_cmd(f"hdfs dfs -put -f {batch_file} {HDFS_BASE}/raw/reviews/", show_output=False)
        elapsed = time.time() - start
        
        if success:
            logger.info(f"[{i:02d}/{total}] Uploaded {batch_file.name} ({elapsed:.2f}s)")
            success_count += 1
        else:
            logger.error(f"[{i:02d}/{total}] FAILED to upload {batch_file.name}")
    
    total_time = time.time() - start_total
    logger.info(f"Upload completed. Success: {success_count}/{total}. Total time: {total_time:.2f}s")

def verify_upload():
    logger.info("=" * 50)
    logger.info("VERIFY UPLOAD RESULTS")
    
    logger.info("--- Metadata Folder ---")
    run_cmd(f"hdfs dfs -ls -h {HDFS_BASE}/raw/meta/")
    
    logger.info("--- Reviews Folder ---")
    # Chỉ hiển thị 5 dòng đầu để tránh spam log nếu quá nhiều file
    run_cmd(f"hdfs dfs -ls -h {HDFS_BASE}/raw/reviews/ | head -n 6")
    
    logger.info("--- Total Size ---")
    run_cmd(f"hdfs dfs -du -h {HDFS_BASE}/raw/")

def main():
    logger.info("=" * 50)
    logger.info("START PROCESS: UPLOAD TO HDFS")
    logger.info("=" * 50)
    
    # 0. Check HDFS
    if not check_hdfs():
        return
    
    # 1. Tạo thư mục
    create_hdfs_dirs()
    
    # 2. Upload metadata
    upload_metadata()
    
    # 3. Upload reviews
    upload_reviews()
    
    # 4. Verify
    verify_upload()
    
    logger.info("=" * 50)
    logger.info("PIPELINE COMPLETED!")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()