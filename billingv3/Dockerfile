# backend/Dockerfile
FROM python:3.10-slim

# Set work directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y \
    libreoffice-writer \
    libreoffice-common \
    fonts-dejavu \
    --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Default command to run
CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]

# Expose port
EXPOSE 8000
