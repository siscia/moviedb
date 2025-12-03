FROM python:3.13
COPY --from=ghcr.io/astral-sh/uv:0.9.15 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/root/.cache/pip \
    uv sync --frozen

COPY src /app/src

CMD ["uv", "run", "streamlit", "run", "/app/src/streamlit_app.py"]
