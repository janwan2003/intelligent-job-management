# Training Runtime Images

Training containers matching ANDREAS job types. Each trains on a real dataset with per-epoch checkpointing.

- Save checkpoint to `/checkpoints/latest.pt` after every epoch
- Load checkpoint on startup if exists (resume)
- No signal handling needed — system kills containers between epochs
- Support `EPOCHS_TOTAL` and `BATCH_SIZE` environment variables

## Available images

| Image | Script | Architecture | Dataset |
|-------|--------|-------------|---------|
| `ijm-lstm-small:dev` | `lstm_small.py` | LSTM (1-layer, 128 hidden) | MNIST |
| `ijm-lstm-big:dev` | `lstm_big.py` | LSTM (3-layer, 256 hidden, dropout) | MNIST |
| `ijm-convnet:dev` | `convnet.py` | ConvNet (3-layer CNN + BN) | CIFAR-10 |
| `ijm-efficientnet:dev` | `efficientnet.py` | MBConv EfficientNet | CIFAR-10 |

## Build

All images use a single Dockerfile with a `SCRIPT` build arg:

```bash
docker build -t ijm-lstm-small:dev --build-arg SCRIPT=lstm_small.py runtime/
docker build -t ijm-lstm-big:dev --build-arg SCRIPT=lstm_big.py runtime/
docker build -t ijm-convnet:dev --build-arg SCRIPT=convnet.py runtime/
docker build -t ijm-efficientnet:dev --build-arg SCRIPT=efficientnet.py runtime/
```

## Test

```bash
mkdir -p /tmp/ckpt /tmp/runs
docker run --rm -v /tmp/ckpt:/checkpoints -v /tmp/runs:/runs \
  -e EPOCHS_TOTAL=3 ijm-lstm-small:dev
```

## Resume

```bash
docker run --rm -v /tmp/ckpt:/checkpoints -v /tmp/runs:/runs \
  -e EPOCHS_TOTAL=5 ijm-lstm-small:dev
# Picks up from epoch 3, trains to 5
```
