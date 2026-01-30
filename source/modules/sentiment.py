import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, udf, coalesce, lit, element_at
)
from pyspark.sql.types import FloatType, StringType

from utils.logger import get_logger

# Set cache directory TRUOC KHI import sparknlp
CACHE_DIR = os.path.expanduser("~/bigdata/models/sparknlp")
os.makedirs(CACHE_DIR, exist_ok=True)
os.environ["SPARK_NLP_CACHE"] = CACHE_DIR

log = get_logger("SentimentAnalyzer")


class SentimentAnalyzer:
    """
    Sentiment Analyzer: Spark NLP (primary) + VADER (fallback)
    
    Usage:
        analyzer = SentimentAnalyzer(spark)
        df = analyzer.analyze(df, text_column="review_text_clean")
    """
    
    def __init__(self, spark, 
                 positive_threshold=0.6, 
                 negative_threshold=0.4,
                 use_sparknlp=True):
        self.spark = spark
        self.positive_threshold = positive_threshold
        self.negative_threshold = negative_threshold
        self.use_sparknlp = use_sparknlp
        self.pipeline = None
        self.model = None
        self.method = None
        
        log.info("Initializing SentimentAnalyzer...")
        log.info(f"  - positive_threshold: {positive_threshold}")
        log.info(f"  - negative_threshold: {negative_threshold}")
        log.info(f"  - use_sparknlp: {use_sparknlp}")
        log.info(f"  - cache_dir: {CACHE_DIR}")
        
        if use_sparknlp:
            self._try_load_sparknlp()
        else:
            self._load_vader()
    
    
    def _try_load_sparknlp(self):
        """Load Spark NLP, fallback to VADER if failed"""
        try:
            log.info("Loading Spark NLP pipeline...")
            
            import sparknlp
            from sparknlp.base import DocumentAssembler
            from sparknlp.annotator import (
                SentimentDLModel,
                UniversalSentenceEncoder
            )
            from pyspark.ml import Pipeline
            
            log.info("  - Spark NLP started")
            
            document = DocumentAssembler() \
                .setInputCol("_text_input") \
                .setOutputCol("document")
            log.info("  - DocumentAssembler created")
            
            # Đường dẫn model LOCAL
            use_path = "/home/thka02415/bigdata/models/sparknlp/tfhub_use"
            sentiment_path = "/home/thka02415/bigdata/models/sparknlp/sentimentdl_use_twitter"
            
            log.info("  - Loading UniversalSentenceEncoder from LOCAL...")
            use = UniversalSentenceEncoder.load(use_path) \
                .setInputCols(["document"]) \
                .setOutputCol("embeddings")
            log.info("  - UniversalSentenceEncoder loaded!")
            
            log.info("  - Loading SentimentDLModel from LOCAL...")
            sentiment = SentimentDLModel.load(sentiment_path) \
                .setInputCols(["embeddings"]) \
                .setOutputCol("sentiment")
            log.info("  - SentimentDLModel loaded!")
            
            self.pipeline = Pipeline(stages=[document, use, sentiment])
            self.method = "sparknlp"
            
            log.info("Spark NLP loaded successfully from LOCAL!")
            
        except Exception as e:
            log.warning(f"Spark NLP failed: {e}")
            self._load_vader()
    
    def _load_vader(self):
        """Load VADER analyzer"""
        log.info("Loading VADER analyzer...")
        self.method = "vader"
        log.info("VADER loaded successfully!")
    
    
    def analyze(self, df, 
                text_column="review_text_clean",
                score_column="sentiment_score",
                label_column="sentiment_label"):
        """
        Analyze sentiment for DataFrame
        
        Returns DataFrame with sentiment_score (0.0-1.0) and sentiment_label
        """
        log.info(f"Analyzing sentiment using {self.method.upper()}...")
        log.info(f"  - Input column: '{text_column}'")
        log.info(f"  - Output columns: '{score_column}', '{label_column}'")
        
        row_count = df.count()
        log.info(f"  - Total rows: {row_count:,}")
        
        if self.method == "sparknlp":
            result = self._analyze_sparknlp(df, text_column, score_column, label_column)
        else:
            result = self._analyze_vader(df, text_column, score_column, label_column)
        
        log.info("Sentiment analysis completed!")
        return result
    
    
    def _analyze_sparknlp(self, df, text_column, score_column, label_column):
        """Analyze using Spark NLP"""
        log.info("  - Preparing input data...")
        df_input = df.withColumn(
            "_text_input",
            coalesce(col(text_column), lit(""))
        )
        
        if self.model is None:
            log.info("  - Fitting Spark NLP model...")
            self.model = self.pipeline.fit(df_input)
            log.info("  - Model fitted!")
        
        log.info("  - Transforming data...")
        df_result = self.model.transform(df_input)
        
        log.info("  - Extracting sentiment results...")
        df_result = df_result \
            .withColumn(
                "_sent_label",
                element_at(col("sentiment.result"), 1)
            ) \
            .withColumn(
                "_sent_confidence",
                coalesce(
                    element_at(col("sentiment.metadata"), 1)
                    .getItem("confidence").cast(FloatType()),
                    lit(0.5)
                )
            )
        
        # Convert to score (0.0 - 1.0) - SỬA: thêm coalesce để handle None
        df_result = df_result.withColumn(
            score_column,
            coalesce(
                when(col("_sent_label") == "positive",
                     (1 + col("_sent_confidence")) / 2)
                .when(col("_sent_label") == "negative",
                      (1 - col("_sent_confidence")) / 2)
                .otherwise(0.5),
                lit(0.5)
            )
        )
        
        df_result = self._add_label_column(df_result, score_column, label_column)
        
        # Cleanup temp columns
        log.info("  - Cleaning up temp columns...")
        columns_to_drop = [
            "_text_input", "document", "embeddings", 
            "sentiment", "_sent_label", "_sent_confidence"
        ]
        for c in columns_to_drop:
            if c in df_result.columns:
                df_result = df_result.drop(c)
        
        return df_result
    
    
    def _analyze_vader(self, df, text_column, score_column, label_column):
        """Analyze using VADER"""
        
        def calc_vader_score(text):
            if text is None or str(text).strip() == "":
                return 0.5
            try:
                from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                analyzer = SentimentIntensityAnalyzer()
                scores = analyzer.polarity_scores(str(text))
                return float((scores['compound'] + 1) / 2)
            except:
                return 0.5
        
        vader_udf = udf(calc_vader_score, FloatType())
        
        log.info("  - Calculating VADER scores...")
        df_result = df.withColumn(score_column, vader_udf(col(text_column)))
        
        log.info("  - Adding sentiment labels...")
        df_result = self._add_label_column(df_result, score_column, label_column)
        
        return df_result
    
    
    def _add_label_column(self, df, score_column, label_column):
        """Add label column based on score thresholds"""
        return df.withColumn(
            label_column,
            when(col(score_column) > self.positive_threshold, "positive")
            .when(col(score_column) < self.negative_threshold, "negative")
            .otherwise("neutral")
        )
    
    
    def get_summary(self, df, label_column="sentiment_label"):
        """Get sentiment statistics"""
        log.info("Calculating sentiment summary...")
        
        total = df.count()
        
        if total == 0:
            log.warning("DataFrame is empty!")
            return {
                "total": 0,
                "positive": 0, "positive_pct": 0,
                "neutral": 0, "neutral_pct": 0,
                "negative": 0, "negative_pct": 0,
                "method": self.method
            }
        
        stats = df.groupBy(label_column).count().collect()
        
        summary = {
            "total": total,
            "positive": 0,
            "neutral": 0,
            "negative": 0,
            "method": self.method
        }
        
        for row in stats:
            label = row[label_column]
            if label in ["positive", "neutral", "negative"]:
                summary[label] = row["count"]
        
        summary["positive_pct"] = round(summary["positive"] * 100 / total, 2)
        summary["neutral_pct"] = round(summary["neutral"] * 100 / total, 2)
        summary["negative_pct"] = round(summary["negative"] * 100 / total, 2)
        
        log.info(f"  - Total: {total:,}")
        log.info(f"  - Positive: {summary['positive']:,} ({summary['positive_pct']}%)")
        log.info(f"  - Neutral: {summary['neutral']:,} ({summary['neutral_pct']}%)")
        log.info(f"  - Negative: {summary['negative']:,} ({summary['negative_pct']}%)")
        
        return summary
    
    
    def print_summary(self, df, label_column="sentiment_label"):
        """Print formatted summary"""
        summary = self.get_summary(df, label_column)
        
        print("\n" + "=" * 50)
        print("SENTIMENT ANALYSIS SUMMARY")
        print("=" * 50)
        print(f"Method:     {summary['method'].upper()}")
        print(f"Total:      {summary['total']:,} reviews")
        print("-" * 50)
        print(f"Positive: {summary['positive']:,} ({summary['positive_pct']}%)")
        print(f"Neutral:  {summary['neutral']:,} ({summary['neutral_pct']}%)")
        print(f"Negative: {summary['negative']:,} ({summary['negative_pct']}%)")
        print("=" * 50 + "\n")
        
        return summary


# ============================================================
#                    STANDALONE FUNCTIONS
# ============================================================

def calc_sentiment(text):
    """Calculate sentiment score using VADER (0.0 to 1.0)"""
    if text is None or str(text).strip() == "":
        return 0.5
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
        scores = analyzer.polarity_scores(str(text))
        return round((scores['compound'] + 1) / 2, 4)
    except:
        return 0.5


def get_sentiment_label(score, pos_thresh=0.6, neg_thresh=0.4):
    """Convert score to label (positive/neutral/negative)"""
    if score is None:
        return "neutral"
    if score > pos_thresh:
        return "positive"
    elif score < neg_thresh:
        return "negative"
    return "neutral"


# ============================================================
#                         TEST
# ============================================================

def test_sentiment():
    """Test sentiment functions with VADER"""
    
    test_cases = [
        ("Amazing food! Best restaurant ever!", "positive"),
        ("Food was okay, nothing special", "neutral"),
        ("Terrible service! Never coming back!", "negative"),
        ("The pizza was great but delivery was slow", "positive"),
        ("Worst experience ever. Cold food, rude staff.", "negative"),
        ("It's fine. Average place.", "neutral"),
        ("Love this place! Will definitely come back!", "positive"),
        ("Not recommended. Waste of money.", "negative"),
        ("", "neutral"),
        (None, "neutral"),
    ]
    
    print("\n" + "=" * 70)
    print("SENTIMENT ANALYSIS TEST (VADER)")
    print("=" * 70)
    
    passed = 0
    failed = 0
    
    for text, expected in test_cases:
        score = calc_sentiment(text)
        label = get_sentiment_label(score)
        
        is_pass = label == expected
        status = "PASS" if is_pass else "FAIL"
        
        if is_pass:
            passed += 1
        else:
            failed += 1
        
        text_display = str(text)[:40] if text else "(empty)"
        # SỬA: Thêm kiểm tra None trước khi format
        score_str = f"{score:.2f}" if score is not None else "N/A"
        print(f"[{status}] '{text_display:40}' -> {score_str} ({label:8}) [expected: {expected}]")
    
    print("=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70 + "\n")
    
    return passed, failed


def test_sentiment_spark():
    """Test sentiment with Spark NLP"""
    
    print("\n" + "=" * 70)
    print("SENTIMENT ANALYSIS TEST (SPARK NLP)")
    print("=" * 70)
    
    # Tao Spark session
    spark = SparkSession.builder \
        .appName("Sentiment_Test") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    # Test data
    test_cases = [
        ("Amazing food! Best restaurant ever!", "positive"),
        ("Food was okay, nothing special", "neutral"),
        ("Terrible service! Never coming back!", "negative"),
        ("The pizza was great but delivery was slow", "positive"),
        ("Worst experience ever. Cold food, rude staff.", "negative"),
        ("It's fine. Average place.", "neutral"),
        ("Love this place! Will definitely come back!", "positive"),
        ("Not recommended. Waste of money.", "negative"),
    ]
    
    # Tao DataFrame
    df = spark.createDataFrame(
        [(text,) for text, _ in test_cases],
        ["review_text_clean"]
    )
    
    # Khoi tao analyzer voi Spark NLP
    analyzer = SentimentAnalyzer(
        spark=spark,
        positive_threshold=0.6,
        negative_threshold=0.4,
        use_sparknlp=True
    )
    
    print(f"\nMethod used: {analyzer.method.upper()}")
    print("-" * 70)
    
    # Analyze
    df_result = analyzer.analyze(df, text_column="review_text_clean")
    
    # Collect results
    results = df_result.select(
        "review_text_clean", 
        "sentiment_score", 
        "sentiment_label"
    ).collect()
    
    # Compare
    passed = 0
    failed = 0
    
    for i, row in enumerate(results):
        text = row["review_text_clean"]
        score = row["sentiment_score"]
        label = row["sentiment_label"]
        expected = test_cases[i][1]
        
        is_pass = label == expected
        status = "PASS" if is_pass else "FAIL"
        
        if is_pass:
            passed += 1
        else:
            failed += 1
        
        text_display = str(text)[:40] if text else "(empty)"
        # SỬA: Thêm kiểm tra None trước khi format
        score_str = f"{score:.2f}" if score is not None else "N/A"
        print(f"[{status}] '{text_display:40}' -> {score_str} ({label:8}) [expected: {expected}]")
    
    print("=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70 + "\n")
    
    # Summary
    analyzer.print_summary(df_result)
    
    spark.stop()
    
    return passed, failed


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("          SENTIMENT ANALYSIS MODULE TEST")
    print("=" * 70)
    
    # Test 1: VADER
    test_sentiment()
    
    # Test 2: Spark NLP
    try:
        test_sentiment_spark()
    except Exception as e:
        print(f"\n[WARNING] Spark NLP test failed: {e}")
        print("Using VADER as fallback.\n")