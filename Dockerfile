# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script and SQL folder
COPY src/ ./src

# Entrypoint to run your ETL script
ENTRYPOINT ["python", "src/data_pipeline.py"]
