FROM nvcr.io/nvidia/cuda:10.1-cudnn7-runtime-ubuntu18.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-setuptools python3-wheel gcc python3-dev libjpeg-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir \
        typing-extensions==3.7.4.3 Pillow==8.4.0 numpy \
        torch==1.7.1+cu101 torchvision==0.8.2+cu101 \
        -f https://download.pytorch.org/whl/torch_stable.html

WORKDIR /app
COPY train_lstm.py .
RUN mkdir -p /checkpoints /runs

CMD ["python3", "-u", "train_lstm.py"]
