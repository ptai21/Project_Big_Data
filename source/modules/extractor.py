from utils.logger import get_logger

log = get_logger("Extractor")

def read_raw_json(spark, path, schema):
    """Đọc JSON từ Raw Zone"""
    log.info(f"Đang đọc Raw JSON từ: {path}")
    # mode="DROPMALFORMED": Bỏ qua dòng lỗi cấu trúc
    return spark.read.schema(schema).option("mode", "DROPMALFORMED").json(path)

def read_processed_parquet(spark, path):
    """Đọc Parquet từ Data Warehouse"""
    log.info(f"Đang đọc Parquet từ: {path}")
    return spark.read.parquet(path)