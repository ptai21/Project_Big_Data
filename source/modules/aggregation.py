"""
Sentiment Aggregation Module

Output:
    - agg_business_sentiment_monthly
    - agg_business_sentiment_yearly  
    - agg_business_sentiment_total
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, sum, avg, min, max,
    when, round, lit
)
from utils.logger import get_logger

log = get_logger("SentimentAggregator")


class SentimentAggregator:
    
    def __init__(self):
        log.info("Initializing SentimentAggregator...")
    
    
    def _base_aggregation(self, df: DataFrame, group_cols: list) -> DataFrame:
        return df.groupBy(*group_cols).agg(
            count("*").alias("total_reviews"),
            sum(when(col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_count"),
            sum(when(col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_count"),
            sum(when(col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_count"),
            round(avg("sentiment_score"), 4).alias("avg_sentiment")
        ).withColumn(
            "positive_pct", 
            round(col("positive_count") * 100 / col("total_reviews"), 2)
        ).withColumn(
            "neutral_pct", 
            round(col("neutral_count") * 100 / col("total_reviews"), 2)
        ).withColumn(
            "negative_pct", 
            round(col("negative_count") * 100 / col("total_reviews"), 2)
        )
    
    
    def create_monthly(self, df: DataFrame) -> DataFrame:
        log.info("Creating MONTHLY aggregation...")
        
        df_prep = df
        if "year" not in df.columns:
            from pyspark.sql.functions import year, month
            df_prep = df.withColumn("year", year(col("time"))) \
                        .withColumn("month", month(col("time")))
        
        df_monthly = self._base_aggregation(df_prep, ["business_id", "year", "month"])
        
        df_monthly = df_monthly.select(
            "business_id", "year", "month",
            "total_reviews",
            "positive_count", "neutral_count", "negative_count",
            "positive_pct", "neutral_pct", "negative_pct",
            "avg_sentiment"
        ).orderBy("business_id", "year", "month")
        
        row_count = df_monthly.count()
        log.info(f"Monthly aggregation created: {row_count:,} rows")
        
        return df_monthly
    
    
    def create_yearly(self, df: DataFrame) -> DataFrame:
        log.info("Creating YEARLY aggregation...")
        
        df_prep = df
        if "year" not in df.columns:
            from pyspark.sql.functions import year
            df_prep = df.withColumn("year", year(col("time")))
        
        df_yearly = self._base_aggregation(df_prep, ["business_id", "year"])
        
        df_yearly = df_yearly.select(
            "business_id", "year",
            "total_reviews",
            "positive_count", "neutral_count", "negative_count",
            "positive_pct", "neutral_pct", "negative_pct",
            "avg_sentiment"
        ).orderBy("business_id", "year")
        
        row_count = df_yearly.count()
        log.info(f"Yearly aggregation created: {row_count:,} rows")
        
        return df_yearly
    
    
    def create_total(self, df: DataFrame) -> DataFrame:
        log.info("Creating TOTAL aggregation...")
        
        df_total = self._base_aggregation(df, ["business_id"])
        
        df_dates = df.groupBy("business_id").agg(
            min("time").cast("date").alias("first_review_date"),
            max("time").cast("date").alias("last_review_date")
        )
        
        df_total = df_total.join(df_dates, on="business_id", how="left")
        
        df_total = df_total.select(
            "business_id",
            "total_reviews",
            "positive_count", "neutral_count", "negative_count",
            "positive_pct", "neutral_pct", "negative_pct",
            "avg_sentiment",
            "first_review_date", "last_review_date"
        ).orderBy("business_id")
        
        row_count = df_total.count()
        log.info(f"Total aggregation created: {row_count:,} rows")
        
        return df_total
    
    
    def create_all(self, df: DataFrame) -> tuple:
        log.info("=" * 50)
        log.info("CREATING ALL SENTIMENT AGGREGATIONS")
        log.info("=" * 50)
        
        required_cols = ["business_id", "time", "sentiment_score", "sentiment_label"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        df.cache()
        total_reviews = df.count()
        log.info(f"Input: {total_reviews:,} reviews")
        
        df_monthly = self.create_monthly(df)
        df_yearly = self.create_yearly(df)
        df_total = self.create_total(df)
        
        df.unpersist()
        
        log.info("ALL AGGREGATIONS COMPLETED!")
        
        return df_monthly, df_yearly, df_total
    
    
    def print_summary(self, df_monthly: DataFrame, df_yearly: DataFrame, df_total: DataFrame):
        print("\n" + "=" * 60)
        print("SENTIMENT AGGREGATION SUMMARY")
        print("=" * 60)
        print(f"MONTHLY:  {df_monthly.count():,} rows")
        print(f"YEARLY:   {df_yearly.count():,} rows")
        print(f"TOTAL:    {df_total.count():,} rows")
        print("=" * 60 + "\n")


def create_sentiment_aggregations(df_reviews: DataFrame) -> tuple:
    aggregator = SentimentAggregator()
    return aggregator.create_all(df_reviews)