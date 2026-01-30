from configs import settings
from utils.logger import get_logger

log = get_logger("Loader")

def write_to_parquet(df, path, partition_col=None):
    """Ghi xuống HDFS"""
    log.info(f"Ghi Parquet xuống: {path}")
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    log.info("-> Ghi Parquet thành công.")

def write_to_postgres(df, table_name):
    """Ghi vào Postgres qua JDBC"""
    log.info(f"Đẩy dữ liệu vào Postgres Table: {table_name}")
    
    jdbc_url = settings.DB_URL
    properties = {
        "user": settings.DB_USER,
        "password": settings.DB_PASS,
        "driver": settings.DB_DRIVER,
        "batchsize": "5000" # Tối ưu tốc độ ghi
    }
    
    try:
        # mode="append": Vì bảng đã được tạo sẵn bởi file init_db.sql
        # Nếu dùng overwrite ở đây, Spark có thể xóa bảng và tạo lại sai kiểu dữ liệu
        df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)
        log.info(f"-> Đẩy vào Postgres bảng {table_name} thành công.")
    except Exception as e:
        log.error(f"Lỗi ghi Postgres: {e}")
        raise e