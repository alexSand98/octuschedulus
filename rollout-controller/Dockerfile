FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY rollout-controller/requirements.txt .
RUN pip install -r requirements.txt

COPY shared /tmp/shared
RUN pip install /tmp/shared && rm -rf /tmp/shared

COPY rollout-controller/ .

EXPOSE 8003
CMD ["python", "main.py"]
