import pathlib

BASE_DIR = pathlib.Path(__file__).parent.parent

class Config:
    HOST = '0.0.0.0'
    PORT = 8080
    BASE_DIR = BASE_DIR
    STATIC_ROOT = BASE_DIR / 'server' / 'static'
    TEMPLATE_ROOT = BASE_DIR / 'templates'
    LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGGING_LEVEL = 'INFO'
