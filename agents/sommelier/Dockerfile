FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY agents/sommelier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY agents/sommelier/app/ sommelier/app/

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8003/metrics')"

CMD ["python", "-m", "sommelier.app.main"]
