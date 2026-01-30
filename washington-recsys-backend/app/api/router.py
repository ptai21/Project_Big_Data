from fastapi import APIRouter
from app.core.config import settings
from app.api.routers import (
    business_router,
    review_router,
    stats_router,
    filter_router
)

api_router = APIRouter(prefix=settings.API_V1_STR)

# Include all routers
api_router.include_router(business_router)
api_router.include_router(review_router)
api_router.include_router(stats_router)
api_router.include_router(filter_router)
