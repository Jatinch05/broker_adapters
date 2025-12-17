"""
Application configuration loaded from environment.
"""
import os


class Settings:
    """Application settings."""
    
    # Database
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://user:password@localhost:5432/broker_adapters"
    )
    
    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # JWT
    JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-prod")
    JWT_ALGORITHM = "HS256"
    JWT_EXPIRY_HOURS = 24
    
    # Payments
    STRIPE_API_KEY = os.getenv("STRIPE_API_KEY", "")
    STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
    RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
    
    # Dhan credentials (example; would be per-user in DB)
    DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "")
    DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
    
    # App
    ENV = os.getenv("ENV", "development")
    DEBUG = ENV == "development"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Broker rate limits (orders per second)
    BROKER_RATE_LIMITS = {
        "dhan": 25,
        "zerodha": 10,
        "angel": 15,
    }
    
    # Data paths
    DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data")
    INSTRUMENTS_CSV = os.path.join(DATA_PATH, "dhan_instruments.csv")


settings = Settings()
