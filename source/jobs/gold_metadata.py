from modules import extractor, loader
from configs import settings
# Lưu ý: Không cần import 'transformer' hay 'schemas' ở đây vì data đã sạch và có schema trong Parquet
from utils.logger import get_logger

# Khởi tạo logger riêng cho job Gold
log = get_logger("Job_Gold_Metadata")

def run(spark):
    log.info("=== BẮT ĐẦU JOB: GOLD METADATA (Parquet -> Postgres) ===")
    
    try:
        # 1. Đọc lại file Parquet từ tầng Silver
        # Việc đọc lại đảm bảo data nhất quán và tận dụng tối ưu hóa của Parquet
        log.info(">>> STEP 1: Reading Processed Data (Silver Layer)")
        
        df_business_gold = extractor.read_processed_parquet(spark, settings.PATH_BUSINESS)
        df_category_gold = extractor.read_processed_parquet(spark, settings.PATH_CATEGORY)
        
        # 2. Ghi vào DB (Gold Layer - Postgres)
        log.info(">>> STEP 2: Loading to PostgreSQL (Gold Layer)")
        
        log.info("Loading DB: BUSINESS...")
        loader.write_to_postgres(df_business_gold, settings.TABLE_BUSINESS)
        
        log.info("Loading DB: CATEGORY...")
        loader.write_to_postgres(df_category_gold, settings.TABLE_CATEGORY)
        
        log.info("=== HOÀN TẤT JOB GOLD ===")
        
    except Exception as e:
        log.error(f"LỖI JOB GOLD (POSTGRES): {e}")
        raise e