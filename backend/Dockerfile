FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend application code
COPY backend/ /app/backend/

# Create data directory
RUN mkdir -p /app/data

ENV PYTHONPATH=/app

EXPOSE 8000

CMD uvicorn backend.app.main:app --host 0.0.0.0 --port ${PORT:-8000}
