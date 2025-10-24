FROM python:3.11-slim

WORKDIR /app

# non-root user
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app

USER appuser

COPY requirements.txt ./
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

EXPOSE 8080

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "src.main"]