FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY agents/historian/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY agents/historian/app/ historian/app/

CMD ["python", "-m", "historian.app.main"]
