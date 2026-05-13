# Training Runtime Images

Training containers matching ANDREAS job types. Each trains on a real dataset with per-epoch checkpointing.

- Save checkpoint to `/checkpoints/latest.pt` after every epoch
- Load checkpoint on startup if exists (resume)
- No signal handling needed — system kills containers between epochs
- Support `EPOCHS_TOTAL` and `BATCH_SIZE` environment variables

## Available images

| Image | Script | Architecture | Dataset | 2-GPU win? |
|-------|--------|-------------|---------|------------|
| `ijm-lstm-small:dev` | `lstm_small.py` | LSTM (1-layer, 128 hidden) | MNIST | no (DP overhead > compute) |
| `ijm-lstm-big:dev` | `lstm_big.py` | LSTM (3-layer, 256 hidden, dropout) | MNIST | P600 marginal (1.10×) |
| `ijm-convnet:dev` | `convnet.py` | ConvNet (3-layer CNN + BN) | CIFAR-10 | no (too light) |
| `ijm-efficientnet:dev` | `efficientnet.py` | MBConv EfficientNet | CIFAR-10 | no (legacy `SiLU` blocks runtime on matemagician) |
| `ijm-cnn_big:dev` | `cnn_big.py` | 10-block deep CNN (channels → 512, 128×128×3) | synthetic | **yes — P600 1.68×, A40 1.12×** |

## Build

All images use a single Dockerfile with a `SCRIPT` build arg:

```bash
docker build -t ijm-lstm-small:dev --build-arg SCRIPT=lstm_small.py runtime/
docker build -t ijm-lstm-big:dev --build-arg SCRIPT=lstm_big.py runtime/
docker build -t ijm-convnet:dev --build-arg SCRIPT=convnet.py runtime/
docker build -t ijm-efficientnet:dev --build-arg SCRIPT=efficientnet.py runtime/
docker build -t ijm-cnn_big:dev --build-arg SCRIPT=cnn_big.py runtime/
```

### Notes on `base.py` cross-version robustness

The legacy CUDA-10.1 image (matemagician) ships PyTorch 1.5.1 / Python 3.7; the modern image ships PyTorch 2.6 / Python 3.13.  Two compatibility shims live in `base.py` so a job can migrate between them mid-run:

- **Dataset load** — `download_dataset` first tries `download=False` (uses pre-staged data at `data/datasets/MNIST/` or `cifar-10-batches-py/`).  Only falls back to `download=True` on `RuntimeError`.  Sidesteps the rootless-docker DNS flakiness on polimi-gpu (slirp4netns nameserver intermittently fails to reach public mirrors).
- **Checkpoint load** — `inner.load_state_dict(..., strict=False)` tolerates minor key differences across torch versions; `optimizer.load_state_dict` is wrapped in a try/except so a Adam-state shape mismatch (legacy 1.5.1 ↔ modern 2.6) restarts the optimizer instead of crashing the trainer.  Model weights still load; the cost is a few epochs of Adam adaptive-rate warmup.
- **Test-loader batch** — capped at `min(256, batch_size)` so heavy CNN models with batch=32 don't OOM the P600's 2-4 GB VRAM during eval.

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
