# Training Runtime Images

Sample training containers that follow the IJM checkpoint contract:
- Write checkpoints to `/checkpoints/latest.pt`
- Load checkpoint on startup if it exists
- Handle SIGTERM/SIGINT gracefully by checkpointing and exiting cleanly
- Support `EPOCHS_TOTAL` and `BATCH_SIZE` environment variables

## Available images

| Image | Script | Architecture | Dataset |
|-------|--------|-------------|---------|
| `ijm-runtime:dev` | `train.py` | LSTM-small (1-layer, 128 hidden) | MNIST |
| `ijm-lstm:dev` | `train_lstm.py` | LSTM-big (3-layer, 256 hidden) | MNIST |
| `ijm-cnn:dev` | `train_cnn.py` | ConvNet (3-layer CNN) | CIFAR-10 |
| `ijm-efficientnet:dev` | `train_efficientnet.py` | MBConv EfficientNet | CIFAR-10 |

---

## 1) Build the images

```bash
docker build -t ijm-runtime:dev runtime/
docker build -t ijm-cnn:dev -f runtime/Dockerfile.cnn runtime/
docker build -t ijm-lstm:dev -f runtime/Dockerfile.lstm runtime/
docker build -t ijm-efficientnet:dev -f runtime/Dockerfile.efficientnet runtime/
```

Verify they exist:

```bash
docker images | grep ijm-
```

---

## 2) Run manually with a checkpoint directory

Create a host directory that will persist checkpoints:

```bash
mkdir -p checkpoints
```

Run any of the images and mount that directory to `/checkpoints`:

```bash
docker run --name ijm-manual-test --rm \
  -v "$(pwd)/checkpoints:/checkpoints" \
  ijm-cnn:dev
```

What to expect:

* It prints progress logs periodically.
* It creates a checkpoint file `latest.pt` in the mounted directory.

---

## 3) Stop it cleanly (trigger SIGTERM + checkpoint)

Open a second terminal and run:

```bash
docker stop -t 30 ijm-manual-test
```

Notes:

* `-t 30` gives it up to 30 seconds to checkpoint and exit cleanly.
* All training scripts handle SIGTERM gracefully.

---

## 4) Confirm it actually checkpointed

On the host:

```bash
ls -lah checkpoints
```

You should see `latest.pt` (possibly a `.tmp` file briefly during atomic writes).

---

## 5) Resume (run again with the same mount)

Just run the same `docker run` command again:

```bash
docker run --name ijm-manual-test --rm \
  -v "$(pwd)/checkpoints:/checkpoints" \
  ijm-cnn:dev
```

What to look for:

* Early in stdout, it prints "Resumed from epoch X".
* Epochs continue from the last saved epoch (not restart from 0).

---
