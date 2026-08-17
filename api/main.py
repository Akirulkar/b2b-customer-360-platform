from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.config import settings
from api.routes import (
    customers,
    intent,
    lead_attribution,
    prioritization,
    sales_performance,
)

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="FastAPI Serving Layer for Spark Gold Marts",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS setup for future Dashboard integration (e.g., Streamlit, React)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root & Health check
@app.get("/", tags=["Health"])
def root():
    return {"message": f"Welcome to {settings.PROJECT_NAME}", "version": settings.VERSION}


@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "healthy"}


# Register API Routers
app.include_router(customers.router, prefix=settings.API_V1_STR)
app.include_router(intent.router, prefix=settings.API_V1_STR)
app.include_router(prioritization.router, prefix=settings.API_V1_STR)
app.include_router(sales_performance.router, prefix=settings.API_V1_STR)
app.include_router(lead_attribution.router, prefix=settings.API_V1_STR)