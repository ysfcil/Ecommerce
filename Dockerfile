# Multi-stage build for Python + Java application (Apache Beam)
FROM python:3.11-slim AS builder

WORKDIR /tmp/build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip, setuptools, wheel (best practice)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Create venv for dependency isolation
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements early for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim

# Install runtime dependencies (Java for Beam)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set Python environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"

WORKDIR /app

# Create non-root user for security
RUN useradd -m -u 1000 appuser

# Copy venv from builder
COPY --from=builder --chown=appuser:appuser /opt/venv /opt/venv

# Copy application code
COPY --chown=appuser:appuser producer.py beam_manager.py reader.py simple_consumer.py .
COPY --chown=appuser:appuser requirements.txt .

# Switch to non-root user
USER appuser

# Health check (simple Python startup check)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command
CMD ["python", "producer.py"]
