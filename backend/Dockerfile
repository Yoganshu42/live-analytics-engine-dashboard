FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Use multiple workers to handle the dashboard's parallel graph requests.
# Tunable via UVICORN_WORKERS; default keeps memory reasonable on small instances.
CMD ["sh", "-lc", "uvicorn main:app --host 0.0.0.0 --port 8000 --workers ${UVICORN_WORKERS:-2}"]
