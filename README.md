# Movie DB
AI powered movie recommendation application

# Usage

Set up environment variables and Nginx configuration:
```bash
cp .env-default .env
# Fill a value for the SECRET_KEY variable in the .env file

cp nginx/moviedb.conf /etc/nginx/sites-enabled/moviedb.conf
service nginx reload

mkdir -p /var/www/moviedb/nginx/
cp nginx/error.html /var/www/moviedb/nginx/error.html
```

To populate and the application, follow these steps:
```bash
docker compose up --build -d
docker compose exec -it streamlit bash

uv run src/manage.py migrate

# Add initial data
uv run src/manage.py import_streaming_availability
uv run src/manage.py build_embeddings

uv run streamlit run src/streamlit_app.py
```
