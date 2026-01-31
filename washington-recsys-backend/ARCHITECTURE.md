# Destination Recommendation System - Architecture Overview

## Tech Stack

**Backend:** FastAPI + SQLAlchemy (async) + PostgreSQL

**Frontend:** React + Tailwind CSS + Recharts

---

## Backend Structure

```
washington-recsys-backend/
├── app/
│   ├── api/
│   │   ├── routers/
│   │   │   ├── business.py
│   │   │   ├── filter.py
│   │   │   ├── review.py
│   │   │   └── stats.py
│   │   └── router.py
│   ├── core/
│   │   ├── config.py
│   │   ├── exceptions.py
│   │   └── middleware.py
│   ├── db/
│   │   └── models/
│   ├── repositories/
│   │   ├── business_repo.py
│   │   ├── log_repo.py
│   │   ├── review_repo.py
│   │   └── stats_repo.py
│   ├── schemas/
│   │   ├── business.py
│   │   ├── filter.py
│   │   ├── responses.py
│   │   ├── review.py
│   │   └── stats.py
│   ├── services/
│   │   ├── business_service.py
│   │   ├── filter_service.py
│   │   ├── review_service.py
│   │   └── stats_service.py
│   ├── database.py
│   └── main.py
└── washington-frontend/
    └── src/
        └── App.js
```

---

## Database Tables

| Table | Description |
|-------|-------------|
| business | Thông tin cửa hàng (name, address, rating, hours...) |
| category | Category flags (food_dining, health_medical...) |
| customer | Thông tin khách hàng |
| review | Reviews với sentiment analysis |
| stats_total | Tổng hợp sentiment theo business |
| stats_yearly | Sentiment theo năm |
| stats_monthly | Sentiment theo tháng |

---

## API Endpoints

### Businesses
- `GET /api/v1/businesses` - List với filter (field, county, city, rating, search, sort_by)
- `GET /api/v1/businesses/{id}` - Chi tiết business

### Reviews
- `GET /api/v1/businesses/{id}/reviews` - List reviews với filter rating
- `GET /api/v1/businesses/{id}/reviews/summary` - Rating distribution (1-5 stars)

### Stats
- `GET /api/v1/businesses/{id}/stats/total` - Tổng sentiment (positive/neutral/negative count & %)
- `GET /api/v1/businesses/{id}/stats/yearly` - Sentiment theo năm
- `GET /api/v1/businesses/{id}/stats/monthly` - Sentiment theo tháng

### Filters
- `GET /api/v1/filters/options` - Lấy danh sách fields, counties
- `GET /api/v1/filters/cities?county=X` - Lấy cities theo county

---

## Frontend Pages

### Page 1: Business List
- Sidebar filters: Field, County, City, Rating
- Sort: by Rating / by Reviews
- Search by name
- Pagination (20/page)

### Page 2: Business Detail
- Info: Description, Category, Location, Hours
- Reviews section với 1 preview + "View all"
- Review Summary (bar chart 1-5 stars)
- Further Analysis (Positive/Neutral/Negative boxes)
- Charts:
  - Average Sentiment Score (line chart)
  - Sentiment Labels Count (3 lines: positive, neutral, negative)
- Dropdown: All (yearly) / Select year (monthly)

### Page 3: Reviews List
- Filter tabs: All, 5⭐, 4⭐, 3⭐, 2⭐, 1⭐
- Review cards với rating, date, text, sentiment label
- Pagination

---

## Data Flow

```
User Action → React State → API Call → FastAPI Router → Service → Repository → PostgreSQL
                                                    ↓
User View   ←  React Render  ←  JSON Response  ←────┘
```

---

## Detailed Flow Examples

### Flow 1: User tìm kiếm businesses

```
1. User chọn filter (Field: Food Dining, County: King County, Rating: 4)
   ↓
2. React cập nhật state filters
   ↓
3. useEffect trigger → fetch GET /api/v1/businesses?field=food_dining&county=King County&min_rating=4
   ↓
4. FastAPI Router (business.py) nhận request
   ↓
5. BusinessService.get_business_list() được gọi
   ↓
6. BusinessRepository.get_list() build SQL query với conditions
   ↓
7. PostgreSQL execute query, trả về List[Business]
   ↓
8. Repository → Service → Router convert sang BusinessListResponse (JSON)
   ↓
9. React nhận response, cập nhật state businesses
   ↓
10. UI render danh sách BusinessCard
```

### Flow 2: User xem chi tiết business

```
1. User click vào BusinessCard
   ↓
2. React set selectedBusiness, chuyển sang DetailView
   ↓
3. useEffect trigger 5 API calls song song:
   - GET /businesses/{id}
   - GET /businesses/{id}/reviews/summary
   - GET /businesses/{id}/stats/total
   - GET /businesses/{id}/stats/yearly
   - GET /businesses/{id}/stats/monthly
   ↓
4. Mỗi Router → Service → Repository → PostgreSQL
   ↓
5. React nhận tất cả responses
   ↓
6. UI render: Info, Hours, Review Summary, Charts
```

### Flow 3: User xem reviews với filter

```
1. User click "View all" → chuyển sang ReviewsPage
   ↓
2. User click tab "5"
   ↓
3. React set filter = "5", reset page = 1
   ↓
4. useEffect trigger → fetch GET /businesses/{id}/reviews?rating=5&page=1
   ↓
5. ReviewRouter nhận request với rating param
   ↓
6. ReviewService.get_reviews_by_business(rating=5)
   ↓
7. ReviewRepository.get_by_business_id() thêm condition: Review.rating == 5
   ↓
8. PostgreSQL trả về chỉ reviews có rating = 5
   ↓
9. React render filtered ReviewCards
```

### Flow 4: User thay đổi sort

```
1. User chọn dropdown "Sort by Reviews"
   ↓
2. React set filters.sort_by = "num_of_reviews"
   ↓
3. useEffect trigger → fetch GET /businesses?sort_by=num_of_reviews
   ↓
4. BusinessRepository.get_list() thêm: ORDER BY num_of_reviews DESC
   ↓
5. PostgreSQL trả về businesses sorted by review count
   ↓
6. UI render danh sách theo thứ tự mới
```
