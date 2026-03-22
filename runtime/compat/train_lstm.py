#!/usr/bin/env python3
"""Self-contained LSTM MNIST trainer for legacy CUDA 10.1 / PyTorch 1.7 environments.

Compatible with Python 3.6+ and PyTorch 1.7+. No dependency on base.py.
Supports checkpointing, resume, and per-epoch logging — same contract as the main runtime.
"""

import logging
import os
import tempfile
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import datasets, transforms

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/checkpoints")
CHECKPOINT_PATH = os.path.join(CHECKPOINT_DIR, "latest.pt")
DATA_ROOT = "/runs/data"
TOTAL_EPOCHS = int(os.environ.get("EPOCHS_TOTAL", "20"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "64"))


class LSTMSmall(nn.Module):
    def __init__(self, input_size=28, hidden_size=128, num_classes=10):
        super(LSTMSmall, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers=1, batch_first=True)
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x):
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


def load_checkpoint(model, optimizer):
    """Load checkpoint if it exists. Returns start epoch."""
    if not os.path.exists(CHECKPOINT_PATH):
        logger.info("No checkpoint found, starting from scratch")
        return 0, 0.0
    logger.info("Loading checkpoint from %s", CHECKPOINT_PATH)
    try:
        checkpoint = torch.load(CHECKPOINT_PATH, map_location="cpu")
        model.load_state_dict(checkpoint["model_state_dict"])
        optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
        epoch = checkpoint["epoch"]
        best_acc = checkpoint.get("best_accuracy", 0.0)
        logger.info("Resumed from epoch %d (best acc: %.2f%%)", epoch, best_acc)
        return epoch, best_acc
    except Exception as e:
        logger.warning("Failed to load checkpoint, starting from scratch: %s", e)
        return 0, 0.0


def save_checkpoint(model, optimizer, epoch, best_accuracy):
    """Save checkpoint atomically."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    checkpoint = {
        "epoch": epoch,
        "model_state_dict": model.state_dict(),
        "optimizer_state_dict": optimizer.state_dict(),
        "best_accuracy": best_accuracy,
    }
    fd, tmp_path = tempfile.mkstemp(dir=CHECKPOINT_DIR, suffix=".pt.tmp")
    try:
        os.close(fd)
        torch.save(checkpoint, tmp_path)
        os.replace(tmp_path, CHECKPOINT_PATH)
        logger.info("Checkpoint saved at epoch %d", epoch)
    except BaseException:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def evaluate(model, test_loader):
    """Compute accuracy on the test set."""
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for images, labels in test_loader:
            images = images.squeeze(1)  # (B,1,28,28) -> (B,28,28)
            outputs = model(images)
            correct += (outputs.argmax(1) == labels).sum().item()
            total += labels.size(0)
    model.train()
    return 100.0 * correct / total


def train():
    logger.info("=" * 60)
    logger.info("Starting LSTM-small training (MNIST, legacy compat)")
    logger.info("Config: epochs_total=%d, batch_size=%d", TOTAL_EPOCHS, BATCH_SIZE)
    logger.info("=" * 60)

    t = transforms.ToTensor()
    train_dataset = datasets.MNIST(DATA_ROOT, train=True, download=True, transform=t)
    test_dataset = datasets.MNIST(DATA_ROOT, train=False, download=True, transform=t)
    train_loader = DataLoader(
        train_dataset, batch_size=BATCH_SIZE, shuffle=True, drop_last=True
    )
    test_loader = DataLoader(test_dataset, batch_size=256, shuffle=False)

    model = LSTMSmall()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    criterion = nn.CrossEntropyLoss()

    current_epoch, best_accuracy = load_checkpoint(model, optimizer)
    logger.info("Starting training from epoch %d to %d", current_epoch, TOTAL_EPOCHS)
    model.train()

    while current_epoch < TOTAL_EPOCHS:
        t0 = time.monotonic()
        total_loss = 0.0
        num_batches = 0
        for images, labels in train_loader:
            images = images.squeeze(1)  # (B,1,28,28) -> (B,28,28)
            optimizer.zero_grad()
            outputs = model(images)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            num_batches += 1

        current_epoch += 1
        avg_loss = total_loss / max(num_batches, 1)
        acc = evaluate(model, test_loader)
        best_accuracy = max(best_accuracy, acc)
        elapsed = time.monotonic() - t0
        logger.info(
            "Epoch %d/%d - Loss: %.6f - Acc: %.2f%% - %.2fs",
            current_epoch,
            TOTAL_EPOCHS,
            avg_loss,
            acc,
            elapsed,
        )
        save_checkpoint(model, optimizer, current_epoch, best_accuracy)

    logger.info("Training completed! Final epoch: %d", current_epoch)


if __name__ == "__main__":
    train()
