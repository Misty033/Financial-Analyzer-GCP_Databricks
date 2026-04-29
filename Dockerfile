FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run expects PORT env variable
# ENV PORT=8080
# EXPOSE 8080

CMD ["python", "pipeline/runner.py"]