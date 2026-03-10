FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Cloud Run expects PORT env var
ENV PORT=8080

# Use gunicorn for production
CMD exec gunicorn --bind :$PORT --workers 1 --threads 2 --timeout 300 app:app