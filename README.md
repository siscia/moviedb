# Movie DB
AI powered movie recommendation application

# Usage
To run the application, follow these steps:

```bash
mv .env-default .env
# Fill a value for the SECRET_KEY variable in the .env file
uv run src/manage.py migrate
uv run src/manage.py runserver
```
