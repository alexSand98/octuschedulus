FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY shared /tmp/shared
RUN pip install /tmp/shared && rm -rf /tmp/shared

COPY loadgen/ .

CMD ["python", "main.py"]
