from pyspark.sql.functions import collect_set, array, explode, coalesce, max, lower, concat_ws, col, udf, when, lit, to_json, from_unixtime, size, array_contains, current_timestamp, year, month, md5, concat_ws, broadcast
from pyspark.sql.types import StringType, MapType, IntegerType, StructType, StructField, ArrayType, DoubleType
from utils import parser
from utils.logger import get_logger
from schemas import tables
import json
import os

# THÊM MỚI
from modules.sentiment import SentimentAnalyzer


# 1. Lấy đường dẫn folder chứa file transformer.py 
# Kết quả: /home/thka02415/bigdata/source/modules
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Đi ngược lên để tìm thư mục gốc dự án ('bigdata')
# Lần 1: Lên 'source'
SOURCE_DIR = os.path.dirname(CURRENT_DIR)

# Lần 2: Lên 'bigdata' (Đây là nơi chứa folder analysis_results)
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

# 3. Nối chuỗi tạo đường dẫn chính xác
# Kết quả mong muốn: /home/thka02415/bigdata/analysis_results/...
PATH_MAPPING = "file://" + os.path.join(PROJECT_ROOT, "analysis_results", "classified_categories_nlp_result.json")


log = get_logger("Transformer")

# Đăng ký UDF (User Defined Functions)
udf_parse_hours = udf(parser.parse_hours, MapType(StringType(), StringType()))
udf_clean_text = udf(parser.clean_text, StringType())
# mới thêm cho phần dăng kí về extract location.
udf_extract_location = udf(parser.extract_location_from_address, MapType(StringType(), StringType()))

def load_category_mapping(spark):
    """
    Hàm đọc file JSON kết quả từ LLM và biến đổi thành DataFrame lookup.
    Output: DataFrame [map_origin, map_group]
    """

    log.info(f"Loading Category Mapping from: {PATH_MAPPING}")

    # Định nghĩa Schema cho file NLP result
    schema_nlp = StructType([
        StructField("details", ArrayType(StructType([
            StructField("original_category", StringType(), True),
            StructField("assigned_group", StringType(), True),
            StructField("confidence_score", DoubleType(), True)
        ])), True)
    ])

    try:
        # Đọc file multi-line JSON
        df_json = spark.read.option("multiline", "true").schema(schema_nlp).json(PATH_MAPPING)
        
        # Bóc tách mảng 'details'
        df_mapping = df_json.select(explode(col("details")).alias("item")) \
                            .select(
                                col("item.original_category").alias("map_origin"),
                                col("item.assigned_group").alias("map_group")
                            ).distinct()
        
        return df_mapping
    except Exception as e:
        log.error(f"Error loading category mapping: {e}")
        raise e
def transform_metadata(df_raw, spark):
    log.info("Starting Meta Data Transformation (Boolean Logic)...")
    
    # --- BƯỚC 0: CHUẨN BỊ DATA MAPPING ---
    df_mapping = load_category_mapping(spark)
    df_mapping = broadcast(df_mapping)

    # --- BƯỚC 1: CLEANING & PREP BASIC INFO ---
    df_clean = df_raw.filter(col("gmap_id").isNotNull())
    
    df_base = df_clean.select(
        col("gmap_id").alias("business_id"),
        # [THAY ĐỔI 1]: Xóa dòng col("gmap_id") bị thừa, vì đã alias ở trên
        col("name"),
        col("description"),
        col("latitude"),
        col("longitude"),
        col("address"),
        col("avg_rating"),
        col("num_of_reviews"),
        col("url"),
        col("category"), 
        concat_ws(", ", col("category")).alias("original_category"),
        udf_extract_location(col("address")).alias("location_info"),
        when(lower(col("state")).contains("permanently closed"), True)
            .otherwise(False).alias("is_permanently_closed"),
        to_json(udf_parse_hours(col("hours"))).alias("hours")
    )
    df_base.cache()

    # --- BƯỚC 2: XỬ LÝ CATEGORY LOGIC (JOIN VỚI MAPPING) ---
    log.info("Mapping Categories...")
    
    df_exploded = df_base.select(
        col("business_id"),
        explode(coalesce(col("category"), array(lit("Uncategorized")))).alias("cat_raw")
    )

    df_joined = df_exploded.join(
        df_mapping,
        df_exploded.cat_raw == df_mapping.map_origin,
        "left"
    )

    # [THAY ĐỔI 2]: Sửa toàn bộ logic Aggregation
    # - Cũ: Dùng first(...) lấy TEXT.
    # - Mới: Dùng max(...) lấy BOOLEAN và collect_set(...) lấy LIST GROUP.
    df_grouped = df_joined.groupBy("business_id").agg(
        # A. Tạo các cột BOOLEAN cho bảng CATEGORY (Nếu có nhóm -> True, ngược lại False)
        max(when(col("map_group") == "Food and Dining", True).otherwise(False)).alias("food_dining"),
        max(when(col("map_group") == "Health and Medical", True).otherwise(False)).alias("health_medical"),
        max(when(col("map_group") == "Automotive and Transport", True).otherwise(False)).alias("automotive_transport"),
        max(when(col("map_group") == "Retail and Shopping", True).otherwise(False)).alias("retail_shopping"),
        max(when(col("map_group") == "Beauty and Wellness", True).otherwise(False)).alias("beauty_wellness"),
        max(when(col("map_group") == "Home Services and Construction", True).otherwise(False)).alias("home_services_construction"),
        max(when(col("map_group") == "Education and Community", True).otherwise(False)).alias("education_community"),
        max(when(col("map_group") == "Entertainment and Travel", True).otherwise(False)).alias("entertainment_travel"),
        max(when(col("map_group") == "Industry and Manufacturing", True).otherwise(False)).alias("industry_manufacturing"),
        max(when(col("map_group") == "Financial and Legal Services", True).otherwise(False)).alias("financial_legal_services"),
        
        # B. Tạo cột new_category cho bảng BUSINESS (Gộp danh sách nhóm)
        # Ví dụ kết quả: "Food and Dining, Retail and Shopping"
        concat_ws(", ", collect_set("map_group")).alias("new_category_list")
    )

    # --- BƯỚC 3: TẠO TABLE BUSINESS ---
    log.info("Building BUSINESS DataFrame...")
    
    df_business = df_base.join(
        # [THAY ĐỔI 3]: Join lấy cột danh sách nhóm vừa tạo
        df_grouped.select("business_id", "new_category_list"),
        on="business_id",
        how="left"
    ).select(
        "business_id", 
        # [THAY ĐỔI 4]: Xóa col("gmap_id") thừa
        "name", "description", 
        "address", 
        lower(col("location_info").getItem("county")).alias("county"),
        lower(col("location_info").getItem("city")).alias("city"),
        "latitude", "longitude", 
        "avg_rating", "num_of_reviews", 
        "url", "is_permanently_closed", "hours",
        "original_category",
        # [THAY ĐỔI 5]: Đổi tên alias từ primary_group thành new_category
        col("new_category_list").alias("new_category")
    )

    # --- BƯỚC 4: TẠO TABLE CATEGORY ---
    log.info("Building CATEGORY DataFrame...")
    
    # [THAY ĐỔI 6]: Chỉ select các cột Boolean (và Business ID) từ bảng đã Group
    df_category = df_grouped.select(
        "business_id",
        "food_dining",
        "health_medical",
        "automotive_transport",
        "retail_shopping",
        "beauty_wellness",
        "home_services_construction",
        "education_community",
        "entertainment_travel",
        "industry_manufacturing",
        "financial_legal_services"
    )

    log.info("Transformation Completed.")
    
    df_base.unpersist()
    df_business = df_business.dropDuplicates(["business_id"])
    return df_business, df_category

def transform_reviews(df_raw, spark, use_sparknlp=True):
    log.info("Starting Reviews Data Transformation...")
    
    df = df_raw.filter(col("gmap_id").isNotNull())
    
    df_trans = df \
        .withColumn("business_id", col("gmap_id")) \
        .withColumn("customer_id", col("user_id")) \
        .withColumn("reviewer_name", col("name")) \
        .withColumn("review_timestamp", (col("time") / 1000).cast("timestamp")) \
        .withColumn("text", udf_clean_text(col("text"))) \
        .withColumn("has_response", col("resp").isNotNull()) \
        .withColumn("response_latency_hrs", 
            when(col("resp").isNotNull(), 
                (col("resp.time") - col("time")) / 3600000
            ).otherwise(None)
        ) \
        .withColumn("review_id", 
            md5(concat_ws("_", col("gmap_id"), col("user_id"), col("time")))
        ) \
        .filter(col("customer_id").isNotNull() & col("business_id").isNotNull() & col("review_timestamp").isNotNull())
    
    # === TÁCH 2 NHÓM ===
    df_with_text = df_trans.filter(col("text").isNotNull() & (col("text") != ""))
    df_null_text = df_trans.filter(col("text").isNull() | (col("text") == ""))
    
    # === NHÓM 1: Có text -> Chạy NLP ===
    log.info("Analyzing sentiment for reviews WITH text...")
    analyzer = SentimentAnalyzer(
        spark=spark,
        positive_threshold=0.6,
        negative_threshold=0.4,
        use_sparknlp=use_sparknlp
    )
    df_analyzed = analyzer.analyze(
        df=df_with_text,
        text_column="text",
        score_column="sentiment_score",
        label_column="sentiment_label"
    )
    
    # === NHÓM 2: Null text -> Dùng Rating ===
    log.info("Inferring sentiment from RATING for reviews WITHOUT text...")
    df_inferred = df_null_text \
        .withColumn("sentiment_score", 
            ((col("rating") - 1) / 4).cast("float")
        ) \
        .withColumn("sentiment_label",
            when(col("rating") > 3.5, "positive")
            .when(col("rating") < 2.5, "negative")
            .otherwise("neutral")
        )
    
    # === GỘP LẠI ===
    log.info("Merging results...")
    df_with_sentiment = df_analyzed.unionByName(df_inferred)
    
    analyzer.print_summary(df_with_sentiment)
    
    # === REVIEW TABLE ===
    df_reviews = df_with_sentiment.select(
        "review_id",
        "business_id",
        "customer_id",
        col("review_timestamp").alias("time"),
        "rating",
        "text",
        "sentiment_score",
        "sentiment_label",
        "has_response",
        "response_latency_hrs"
    ).dropDuplicates(["review_id"])
    
    # === CUSTOMER TABLE ===
    df_customer = df_with_sentiment.select(
        "customer_id",
        col("reviewer_name").alias("name")
    ).dropDuplicates(["customer_id"])

    return df_reviews, df_customer
