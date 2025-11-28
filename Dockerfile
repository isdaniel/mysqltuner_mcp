FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
COPY src/ ./src/
COPY pyproject.toml .

RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

# Default entrypoint
ENTRYPOINT ["python", "-m", "mysqltuner_mcp"]

# Default to stdio mode
CMD ["--mode", "stdio"]
