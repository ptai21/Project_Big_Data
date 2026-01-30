from modules import extractor, transformer, loader
from configs import settings
from schemas import tables
from utils.logger import get_logger

# Logger riêng cho job Silver Reviews
log = get_logger("Job_Silver_Reviews")

def run(spark):
    log.info("=== BẮT ĐẦU JOB: SILVER REVIEWS (Raw -> Parquet) ===")

    try:
        # 1. Đọc Raw JSON
        log.info(">>> STEP 1: Reading Raw Data")
        df_raw = extractor.read_raw_json(spark, settings.PATH_RAW_REVIEWS, tables.SCHEMA_RAW_REVIEWS)
        
        # 2. Xử lý logic (Convert Time, Calc Latency) và tách bảng
        log.info(">>> STEP 2: Transforming Data")
        df_reviews, df_customer = transformer.transform_reviews(df_raw, spark)
        
        # [QUAN TRỌNG] Cache lại trước khi Ghi
        # Reviews thường có dung lượng lớn, việc cache cực kỳ quan trọng ở bước này
        df_reviews.cache()
        df_customer.cache()

        # 3. Lưu xuống HDFS (Silver Layer - Parquet)
        log.info(">>> STEP 3: Writing to HDFS (Silver Layer)")
        
        log.info("Writing Parquet: REVIEWS")
        loader.write_to_parquet(df_reviews, settings.PATH_REVIEWS)
        
        log.info("Writing Parquet: CUSTOMER")
        loader.write_to_parquet(df_customer, settings.PATH_CUSTOMER)

        # Giải phóng RAM
        df_reviews.unpersist()
        df_customer.unpersist()
        
        log.info("=== HOÀN TẤT JOB SILVER REVIEWS ===")

    except Exception as e:
        log.critical(f"LỖI JOB SILVER REVIEWS: {e}")
        try:
            df_reviews.unpersist()
            df_customer.unpersist()
        except:
            pass
        raise e