FROM python:3.11-slim

LABEL maintainer="XCNStock"
LABEL description="选股微服务 API"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

ARG UID=502
ARG GID=20

RUN groupadd -g $GID appuser || echo "Group already exists"
RUN useradd -u $UID -g $GID -m appuser || echo "User already exists"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nextai/ ./nextai/
COPY core/ ./core/
COPY filters/ ./filters/
COPY config/ ./config/
COPY services/ ./services/
COPY models/ ./models/
COPY factors/ ./factors/

RUN mkdir -p /app/data/kline /app/data/limitup /app/logs && chown -R appuser:$GID /app/data /app/logs

ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai
ENV FLASK_APP=nextai.api.app

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER appuser

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
