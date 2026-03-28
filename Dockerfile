FROM python:3.11-slim

LABEL maintainer="XCNStock"
LABEL description="A股历史数据采集服务"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 创建与宿主机相同 UID/GID 的用户
ARG UID=502
ARG GID=20

# 先创建用户，使用 GID 20
RUN groupadd -g $GID appuser || echo "Group already exists"
RUN useradd -u $UID -g $GID -m appuser || echo "User already exists"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
COPY services/ ./services/
COPY models/ ./models/
COPY config/ ./config/
COPY core/ ./core/
COPY factors/ ./factors/
COPY filters/ ./filters/
COPY optimization/ ./optimization/
COPY patterns/ ./patterns/

RUN mkdir -p /app/data/kline /app/logs && chown -R appuser:$GID /app/data /app/logs

ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER appuser

CMD ["python", "scripts/scheduled_fetch.py"]
