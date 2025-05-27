import os

# Superset specific config for Weather Analytics
ROW_LIMIT = 10000
SUPERSET_WEBSERVER_PORT = 8088

# Flask App Builder configuration
APP_NAME = "Weather Analytics Dashboard"
SECRET_KEY = 'weather_analytics_secret_key_2024'

SQLALCHEMY_DATABASE_URI = 'postgresql://airflow:airflow@postgres:5432/superset'
SQLALCHEMY_TRACK_MODIFICATIONS = True

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24 * 365

# Cache config
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'superset-redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
    'CACHE_REDIS_URL': 'redis://superset-redis:6379/1'
}

# Database connections for weather analytics
DATABASE_CONNECTIONS = {
    'weather_analytics': 'postgresql://airflow:airflow@postgres:5432/airflow'
}

# Weather-specific configurations
WEATHER_DATA_REFRESH_INTERVAL = 3600  # 1 hour
DEFAULT_WEATHER_TIMEZONE = 'Asia/Jakarta'

# Webserver configuration
ENABLE_PROXY_FIX = True
WEBSERVER_THREADS = 8

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'ENABLE_TEMPLATE_REMOVE_FILTERS': True,
    'ENABLE_TEMPLATE_EDITOR': True,
}

# Visualization configuration
VIZ_TYPE_BLACKLIST = []
ENABLE_JAVASCRIPT_CONTROLS = False
