FROM python:3.9-slim

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apscheduler


COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "run_etl.py"]