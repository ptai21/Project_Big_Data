import sys
from pyspark.sql import SparkSession
from configs import settings
from utils.logger import get_logger

# Import các module đã tách biệt
from jobs import silver_metadata, gold_metadata, silver_reviews, gold_reviews

# Đặt tên logger bằng tiếng Anh
log = get_logger("Main_Orchestrator")

def create_spark_session():
    """Khởi tạo Spark với Driver Postgres"""
    log.info(f"Initializing Spark Session. Jar Path: {settings.JAR_PATH}")

    # Cấu hình tối ưu (có thể thêm partition nếu dữ liệu lớn)
    return SparkSession.builder \
        .appName(settings.APP_NAME) \
        .config("spark.jars", settings.JAR_PATH) \
        .config("spark.driver.extraClassPath", settings.JAR_PATH) \
        .config("spark.executor.extraClassPath", settings.JAR_PATH) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def main():
    log.info(">>>>>>>> STARTING ETL SYSTEM (MEDALLION ARCHITECTURE) <<<<<<<<")
    
    spark = create_spark_session()
    
    try:
        # --- PHASE 1: METADATA PIPELINE (Business, Category) ---
        # Chạy trước để đảm bảo Business ID đã tồn tại trong DB cho Foreign Key
        log.info("==============================================")
        log.info("STARTING PIPELINE: METADATA (DIMENSIONS)")
        log.info("==============================================")
        
        # 1.1 Metadata Silver (Raw -> Parquet)
        silver_metadata.run(spark)
        
        # 1.2 Metadata Gold (Parquet -> Postgres)
        gold_metadata.run(spark)
        
        
        # --- PHASE 2: REVIEWS PIPELINE (Reviews, Users) ---
        # Chạy sau khi Metadata đã hoàn tất
        log.info("==============================================")
        log.info("STARTING PIPELINE: REVIEWS (FACTS)")
        log.info("==============================================")
        
        # 2.1 Reviews Silver (Raw -> Parquet)
        silver_reviews.run(spark)
        
        # 2.2 Reviews Gold (Parquet -> Postgres)
        gold_reviews.run(spark)
        
        log.info(">>>>>>>> CONGRATULATIONS! ALL PIPELINES COMPLETED SUCCESSFULLY <<<<<<<<")
        
    except Exception as e:
        # Ghi log lỗi nghiêm trọng và thoát chương trình với mã lỗi 1
        log.critical(f"SYSTEM STOPPED DUE TO ERROR: {e}")
        sys.exit(1)
    
    finally:
        # Luôn đảm bảo đóng Spark session dù thành công hay thất bại
        spark.stop()
        log.info("Spark Session closed.")

if __name__ == "__main__":
    main()