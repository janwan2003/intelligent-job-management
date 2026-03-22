FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared base + specific training script
ARG SCRIPT=lstm_small.py
COPY base.py .
COPY ${SCRIPT} ./train.py

RUN mkdir -p /checkpoints /runs

CMD ["python", "-u", "train.py"]
