# app/database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import settings

# 1. Tạo Engine kết nối Sync
# Lưu ý: DATABASE_URL trong config phải là dạng "postgresql://..." 
# (hoặc "postgresql+psycopg2://..."), KHÔNG dùng "postgresql+asyncpg://..."
engine = create_engine(
    settings.DATABASE_URL, 
    pool_pre_ping=True, # Tự động kiểm tra kết nối sống trước khi query
    echo=False
)

# 2. Tạo Session Factory
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)

# 3. Base Model
Base = declarative_base()

# 4. Dependency Injection (Sync)
# Hàm này sẽ được gọi trong mỗi Request của FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()