from superset.config import *

ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres:changeme_strong_password@postgres-central:5432/lakehouse'
SECRET_KEY = '{{ secret_key }}'
