import os

SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI', 'sqlite:////app/superset_home/superset.db')
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'your_secret_key_here')

CACHE_CONFIG = {
    'CACHE_TYPE': os.getenv('SUPERSET_CACHE_TYPE', 'RedisCache'),
    'CACHE_DEFAULT_TIMEOUT': int(os.getenv('SUPERSET_CACHE_DEFAULT_TIMEOUT', 86400)),
    'CACHE_KEY_PREFIX': 'superset_cache',
    'CACHE_REDIS_URL': os.getenv('SUPERSET_CACHE_REDIS_URL', 'redis://redis:6379/1')
}
DATA_CACHE_CONFIG = CACHE_CONFIG
