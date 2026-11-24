FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY docs ./docs
COPY tests ./tests
COPY scripts ./scripts
COPY audit ./audit
COPY pyproject.toml .
RUN mkdir -p /app/data
ENV PYTHONUNBUFFERED=1 JOB_DB=/app/data/jobs.db
CMD ["python", "-m", "uvicorn", "src.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
