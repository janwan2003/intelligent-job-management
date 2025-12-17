FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY train.py .

# Create directories for checkpoints and runs
RUN mkdir -p /checkpoints /runs

CMD ["python", "-u", "train.py"]
