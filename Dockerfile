# API мини-CRM (FastAPI). GUI Tkinter в контейнер не входит — подключайте клиент с хоста.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY scripts ./scripts

RUN mkdir -p /app/data

EXPOSE 8000

CMD ["uvicorn", "src.backend.crm_api:app", "--host", "0.0.0.0", "--port", "8000"]
