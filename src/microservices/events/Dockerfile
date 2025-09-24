FROM python:3.11-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY events-service.py .

EXPOSE 8082

CMD ["python", "events-service.py"]