from pyspark.sql.types import (StructType, StructField, StringType, 
                               FloatType, IntegerType, LongType, 
                               ArrayType, MapType)

# ====================================================
# A. SCHEMA INPUT (Dữ liệu JSON thô)
# ====================================================

# 1. Schema cho dữ liệu Metadata (Business)
SCHEMA_RAW_META = StructType([
    StructField("gmap_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("category", ArrayType(StringType()), True),
    StructField("avg_rating", FloatType(), True),
    StructField("num_of_reviews", IntegerType(), True),
    StructField("price", StringType(), True),
    StructField("hours", ArrayType(ArrayType(StringType())), True),
    StructField("MISC", MapType(StringType(), ArrayType(StringType())), True),
    StructField("state", StringType(), True),
    StructField("url", StringType(), True)
])

# 2. Schema cho phản hồi của doanh nghiệp (Phần con trong Review)
SCHEMA_RESP = StructType([
    StructField("time", LongType(), True),
    StructField("text", StringType(), True)
])

# 3. Schema cho dữ liệu Reviews
SCHEMA_RAW_REVIEWS = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("time", LongType(), True),
    StructField("rating", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("gmap_id", StringType(), True),
    StructField("resp", SCHEMA_RESP, True)
])

# ====================================================
# B. SCHEMA OUTPUT (Danh sách cột cho Database)
# ====================================================

# Các cột cho bảng BUSINESS
COLS_BUSINESS = [
    "gmap_id", "name", "description", "latitude", "longitude",
    "avg_rating", "num_of_reviews", "url", "city",
    "is_permanently_closed", "hours"
]

# Các cột cho bảng REVIEW 
COLS_REVIEW = [
    "business_key",        
    "user_id",
    "review_timestamp",     
    "reviewer_name",       
    "rating",
    "review_text_clean", 
    "sentiment_score",    
    "sentiment_label",     
    "has_response",         
    "response_latency_hrs",
    "review_date",        
    "review_year",     
    "review_month"  
]

# Các cột cho bảng CUSTOMER
COLS_CUSTOMER = ["user_id", "name"]

# Các cột cho bảng CATEGORY 
COLS_CATEGORY_MAP = ["gmap_id", "category_name"]