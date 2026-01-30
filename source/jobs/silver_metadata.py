from modules import extractor, transformer, loader
from configs import settings
from schemas import tables
from utils.logger import get_logger

# Khởi tạo logger riêng cho job Silver
log = get_logger("Job_Silver_Metadata")

def run(spark):
    log.info("=== BẮT ĐẦU JOB: SILVER METADATA (Raw -> Parquet) ===")
    
    try:
        # 1. Đọc Raw JSON
        log.info(">>> STEP 1: Reading Raw Data")
        df_raw = extractor.read_raw_json(spark, settings.PATH_RAW_META, tables.SCHEMA_RAW_META)
        
        # 2. Xử lý logic (Clean, Normalize, Split tables)
        log.info(">>> STEP 2: Transforming Data")
        df_business, df_category = transformer.transform_metadata(df_raw, spark)
        
        # [QUAN TRỌNG] Cache lại trước khi Ghi
        # Cache để tránh Spark đọc lại file Raw JSON 3 lần cho 3 bảng output
        df_business.cache()
        df_category.cache()
        
        # 3. Lưu xuống HDFS (Silver Layer - Parquet)
        log.info(">>> STEP 3: Writing to HDFS (Silver Layer)")
        
        log.info("Writing Parquet: BUSINESS")
        loader.write_to_parquet(df_business, settings.PATH_BUSINESS)
        
        log.info("Writing Parquet: CATEGORY")
        loader.write_to_parquet(df_category, settings.PATH_CATEGORY)
        
        # Giải phóng RAM
        df_business.unpersist()
        df_category.unpersist()
        
        log.info("=== HOÀN TẤT JOB SILVER ===")
        
    except Exception as e:
        log.critical(f"LỖI JOB SILVER: {e}")
        # Đảm bảo unpersist nếu có lỗi xảy ra để tránh leak memory (optional nhưng recommend)
        try:
            df_business.unpersist()
            df_category.unpersist()
        except:
            pass
        raise e