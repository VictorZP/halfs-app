FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app/backend/

# Create data directory
RUN mkdir -p /app/data

ENV PYTHONPATH=/app
ENV DATABASE_URL=sqlite:///./data/app.db

EXPOSE 8000

CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
