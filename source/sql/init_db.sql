-- 1. CLEAN UP (Xóa bảng cũ để tạo lại)
DROP TABLE IF EXISTS REVIEW CASCADE;
DROP TABLE IF EXISTS CATEGORY CASCADE;
DROP TABLE IF EXISTS BUSINESS CASCADE;
DROP TABLE IF EXISTS CUSTOMER CASCADE;
DROP TABLE IF EXISTS STATS_MONTHLY CASCADE;
DROP TABLE IF EXISTS STATS_YEARLY CASCADE;
DROP TABLE IF EXISTS STATS_TOTAL CASCADE;
-- 2. CREATE TABLES
-- 2.1 Table: CUSTOMER
CREATE TABLE CUSTOMER (
    customer_id TEXT PRIMARY KEY,
    name        TEXT
);

-- 2.2 Table: BUSINESS (Đã cập nhật thêm address và county)
CREATE TABLE BUSINESS (
    business_id           TEXT PRIMARY KEY,
    name                  TEXT,
    description           TEXT,
    
    -- Location Info
    address               TEXT,
    county                TEXT,           -- Dùng để lọc khu vực (Quận/Huyện)
    city                  VARCHAR(100),
    latitude              DECIMAL(10, 7),
    longitude             DECIMAL(10, 7),
    
    -- Metrics
    avg_rating            DECIMAL(3, 2),
    num_of_reviews        INT DEFAULT 0,
    
    -- Details
    url                   TEXT,
    is_permanently_closed BOOLEAN DEFAULT FALSE,
    hours                 TEXT, -- Lưu giờ mở cửa (có thể là JSON string)
    
    -- Category Info (Lưu text gốc để hiển thị)
    original_category     TEXT,
    new_category          TEXT
);

-- 2.3 Table: CATEGORY (Extension 1:1)
CREATE TABLE CATEGORY (
    business_id                 TEXT PRIMARY KEY,
    food_dining                 BOOLEAN DEFAULT FALSE,
    health_medical              BOOLEAN DEFAULT FALSE,
    automotive_transport        BOOLEAN DEFAULT FALSE,
    retail_shopping             BOOLEAN DEFAULT FALSE,
    beauty_wellness             BOOLEAN DEFAULT FALSE,
    home_services_construction  BOOLEAN DEFAULT FALSE,
    education_community         BOOLEAN DEFAULT FALSE,
    entertainment_travel        BOOLEAN DEFAULT FALSE,
    industry_manufacturing      BOOLEAN DEFAULT FALSE,
    financial_legal_services    BOOLEAN DEFAULT FALSE,
    
    CONSTRAINT fk_category_business 
        FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id) 
        ON DELETE CASCADE
);

-- -- 2.4 Table: MISC (Extension 1:1)
-- CREATE TABLE MISC (
--     business_id  VARCHAR(255) PRIMARY KEY,
    
--     -- [QUAN TRỌNG] Dùng JSONB để đánh Index tìm kiếm. 
--     -- Ví dụ dữ liệu: ["wifi", "credit_cards", "wheelchair"]
--     search_tags  JSONB DEFAULT '[]'::jsonb, 
    
--     -- Lưu chi tiết hiển thị (cấu trúc cây)
--     meta_data    JSONB DEFAULT '{}'::jsonb,
    
--     CONSTRAINT fk_misc_business 
--         FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id) 
--         ON DELETE CASCADE
-- );

-- 2.5 Table: REVIEW
CREATE TABLE REVIEW (
    review_id            TEXT PRIMARY KEY,
    business_id          TEXT NOT NULL,
    customer_id          TEXT NOT NULL,
    
    -- Thời gian gốc
    time                 TIMESTAMP WITHOUT TIME ZONE,
    
    -- [TỰ ĐỘNG] Các cột tách ra để Report nhanh (Partitioning Support)
    -- Postgres tự tính toán, dữ liệu luôn chính xác 100%
    date                 DATE GENERATED ALWAYS AS (time::date) STORED,
    month                INT GENERATED ALWAYS AS (EXTRACT(MONTH FROM time)) STORED,
    year                 INT GENERATED ALWAYS AS (EXTRACT(YEAR FROM time)) STORED,
    
    -- Content & Sentiment
    rating               INT CHECK (rating >= 1 AND rating <= 5),
    text                 TEXT,
    sentiment_score      DECIMAL(5, 4),
    sentiment_label      VARCHAR(50),
    has_response         BOOLEAN DEFAULT FALSE,
    response_latency_hrs DECIMAL(10, 2),
    
    CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id),
    CONSTRAINT fk_review_customer FOREIGN KEY (customer_id) REFERENCES CUSTOMER(customer_id)
);
-- 2.5 Table: STATS_MONTHLY (Thống kê theo Tháng)
CREATE TABLE STATS_MONTHLY (
    business_id     TEXT NOT NULL,
    year            INT NOT NULL,
    month           INT NOT NULL,
    
    total_reviews   INT DEFAULT 0,
    positive_count  INT DEFAULT 0,
    neutral_count   INT DEFAULT 0,
    negative_count  INT DEFAULT 0,
    
    positive_pct    DECIMAL(5, 2), -- Ví dụ: 85.50 (%)
    neutral_pct     DECIMAL(5, 2),
    negative_pct    DECIMAL(5, 2),
    
    avg_sentiment   DECIMAL(5, 4), -- Điểm cảm xúc trung bình tháng đó
    
    -- Khóa chính phức hợp: Mỗi quán, trong 1 tháng, chỉ có 1 dòng thống kê
    PRIMARY KEY (business_id, year, month),
    
    CONSTRAINT fk_stats_monthly_biz 
        FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id) 
        ON DELETE CASCADE
);
CREATE TABLE STATS_YEARLY (
    business_id     TEXT NOT NULL,
    year            INT NOT NULL,
    
    total_reviews   INT DEFAULT 0,
    positive_count  INT DEFAULT 0,
    neutral_count   INT DEFAULT 0,
    negative_count  INT DEFAULT 0,
    
    positive_pct    DECIMAL(5, 2),
    neutral_pct     DECIMAL(5, 2),
    negative_pct    DECIMAL(5, 2),
    
    avg_sentiment   DECIMAL(5, 4),
    
    PRIMARY KEY (business_id, year),
    
    CONSTRAINT fk_stats_yearly_biz 
        FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id) 
        ON DELETE CASCADE
);
-- 2.7 Table: STATS_TOTAL (Thống kê tổng thể từ trước đến nay)
CREATE TABLE STATS_TOTAL (
    business_id     TEXT PRIMARY KEY, -- Mỗi quán chỉ có 1 dòng tổng
    
    total_reviews   INT DEFAULT 0,
    positive_count  INT DEFAULT 0,
    neutral_count   INT DEFAULT 0,
    negative_count  INT DEFAULT 0,
    
    positive_pct    DECIMAL(5, 2),
    neutral_pct     DECIMAL(5, 2),
    negative_pct    DECIMAL(5, 2),
    
    avg_sentiment   DECIMAL(5, 4),
    
    first_review_date DATE, -- Ngày review đầu tiên
    last_review_date  DATE, -- Ngày review gần nhất
    
    CONSTRAINT fk_stats_total_biz 
        FOREIGN KEY (business_id) REFERENCES BUSINESS(business_id) 
        ON DELETE CASCADE
);

-- 3. INDEXING STRATEGY (Tối ưu cho luồng Lọc -> Search)
-- 3.1 PARTIAL INDEXES cho bảng CATEGORY (Bước 1: Lọc Nhóm)
-- Chỉ đánh index cho giá trị TRUE (giảm dung lượng index ~80-90%)
CREATE INDEX idx_cat_food ON CATEGORY(food_dining) WHERE food_dining = TRUE;
CREATE INDEX idx_cat_health ON CATEGORY(health_medical) WHERE health_medical = TRUE;
CREATE INDEX idx_cat_auto ON CATEGORY(automotive_transport) WHERE automotive_transport = TRUE;
CREATE INDEX idx_cat_retail ON CATEGORY(retail_shopping) WHERE retail_shopping = TRUE;
CREATE INDEX idx_cat_beauty ON CATEGORY(beauty_wellness) WHERE beauty_wellness = TRUE;
CREATE INDEX idx_cat_home ON CATEGORY(home_services_construction) WHERE home_services_construction = TRUE;
CREATE INDEX idx_cat_edu ON CATEGORY(education_community) WHERE education_community = TRUE;
CREATE INDEX idx_cat_travel ON CATEGORY(entertainment_travel) WHERE entertainment_travel = TRUE;
CREATE INDEX idx_cat_industry ON CATEGORY(industry_manufacturing) WHERE industry_manufacturing = TRUE;
CREATE INDEX idx_cat_finance ON CATEGORY(financial_legal_services) WHERE financial_legal_services = TRUE;

-- 3.2 GIN INDEX cho bảng MISC (Bước 2: Search chi tiết)
-- Giúp tìm kiếm "Có wifi không?", "Có parking không?" siêu tốc
-- CREATE INDEX idx_misc_tags ON MISC USING GIN (search_tags);

-- 3.3 INDEX cho bảng BUSINESS (Địa lý & Tìm kiếm tên)
CREATE INDEX idx_business_county ON BUSINESS(county);  -- Lọc theo Quận/Huyện
CREATE INDEX idx_business_city ON BUSINESS(city);      -- Lọc theo Thành phố
-- Full-Text Search cho tên quán (VD: tìm "Phở Hùng")
CREATE INDEX idx_business_name_search ON BUSINESS USING GIN (to_tsvector('simple', name));

-- 3.4 INDEX cho bảng REVIEW (Phân tích & Thống kê)
CREATE INDEX idx_review_business_id ON REVIEW(business_id);
CREATE INDEX idx_review_customer_id ON REVIEW(customer_id);
-- Hỗ trợ thống kê theo thời gian (VD: Doanh thu tháng 10/2024)
CREATE INDEX idx_review_year_month ON REVIEW(year, month);
-- Hỗ trợ lấy review mới nhất của 1 quán
CREATE INDEX idx_review_biz_time ON REVIEW(business_id, time DESC);
-- [THÊM MỚI] INDEX CHO BẢNG THỐNG KÊ

-- 1. Index cho STATS_MONTHLY
-- Giúp query: "Lấy tình hình kinh doanh tháng 10/2025 của tất cả các quán"
CREATE INDEX idx_stats_monthly_time ON STATS_MONTHLY(year, month);
-- Giúp query: "Top các quán có điểm sentiment cao nhất trong tháng X"
CREATE INDEX idx_stats_monthly_rank ON STATS_MONTHLY(year, month, avg_sentiment DESC);

-- 2. Index cho STATS_YEARLY
-- Giúp query: "Báo cáo năm 2024"
CREATE INDEX idx_stats_yearly_time ON STATS_YEARLY(year);
-- Giúp query: "Top quán hot nhất năm (nhiều review nhất)"
CREATE INDEX idx_stats_yearly_rank ON STATS_YEARLY(year, total_reviews DESC);

-- 3. Index cho STATS_TOTAL
-- Giúp query: "Top quán uy tín nhất toàn sàn (Avg Sentiment cao nhất)"
CREATE INDEX idx_stats_total_sentiment ON STATS_TOTAL(avg_sentiment DESC);
-- Giúp query: "Top quán nổi tiếng nhất (Nhiều review nhất)"
CREATE INDEX idx_stats_total_reviews ON STATS_TOTAL(total_reviews DESC);