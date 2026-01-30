from modules import extractor, loader
from configs import settings
# Không cần import transformer hay schemas vì dùng lại data chuẩn từ Silver
from utils.logger import get_logger
from modules.aggregation import create_sentiment_aggregations

# Logger riêng cho job Gold Reviews
log = get_logger("Job_Gold_Reviews")

def run(spark):
    log.info("=== BẮT ĐẦU JOB: GOLD REVIEWS (Parquet -> Postgres) ===")

    try:
        # 1. Đọc lại Parquet từ tầng Silver
        log.info(">>> STEP 1: Reading Processed Data (Silver Layer)")
        df_reviews_gold = extractor.read_processed_parquet(spark, settings.PATH_REVIEWS)
        df_customer_gold = extractor.read_processed_parquet(spark, settings.PATH_CUSTOMER)
        
        # 2. Tao Aggregations
        log.info(">>> STEP 2: Creating Sentiment Aggregations")
        df_monthly, df_yearly, df_total = create_sentiment_aggregations(df_reviews_gold)

        # 3. Ghi vào DB (Gold Layer - Postgres)
        log.info(">>> STEP 3: Loading to PostgreSQL (Gold Layer)")
        
        # [Lưu ý] Nên load Customer trước Reviews
        # Nếu trong DB có setup Foreign Key (Review thuộc về Customer), 
        # thì Customer phải tồn tại trước mới insert được Review.
        log.info("Loading DB: CUSTOMER...")
        loader.write_to_postgres(df_customer_gold, settings.TABLE_CUSTOMER)
        
        log.info("Loading DB: REVIEWS...")
        loader.write_to_postgres(df_reviews_gold, settings.TABLE_REVIEWS)
        
        log.info("Loading: AGG_SENTIMENT_MONTHLY...")
        loader.write_to_postgres(df_monthly, settings.TABLE_MONTHLY)
        
        log.info("Loading: AGG_SENTIMENT_YEARLY...")
        loader.write_to_postgres(df_yearly, settings.TABLE_YEARLY)
        
        log.info("Loading: AGG_SENTIMENT_TOTAL...")
        loader.write_to_postgres(df_total, settings.TABLE_TOTAL)
        
        log.info("=== HOÀN TẤT JOB GOLD REVIEWS ===")
        
    except Exception as e:
        log.error(f"LỖI JOB GOLD REVIEWS (POSTGRES): {e}")
        raise e