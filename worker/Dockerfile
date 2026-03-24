FROM python:3.13-slim

WORKDIR /app

# Install docker CLI (for running docker commands)
RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://get.docker.com -o get-docker.sh && \
    sh get-docker.sh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* get-docker.sh

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker/app.py worker/constants.py ./
COPY shared/ ./shared/

EXPOSE 8001

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
