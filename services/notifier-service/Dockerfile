FROM python:3.11-slim
WORKDIR /app

COPY services/notifier-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Common shared code
COPY services/common /app/services/common
# Service code
COPY services/notifier-service/app /app/app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "-m", "app.main"]
