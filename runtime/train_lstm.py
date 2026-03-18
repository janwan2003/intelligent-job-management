#!/usr/bin/env python3
"""
LSTM-big: 3-layer LSTM with dropout on MNIST (28-step sequences of 28 features).

Follows the IJM checkpoint contract:
- Loads checkpoint from /checkpoints/latest.pt on startup if it exists
- Saves checkpoint after every epoch
- Handles SIGTERM/SIGINT gracefully by checkpointing and exiting cleanly
"""

import contextlib
import logging
import os
import tempfile
import time
from pathlib import Path

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


class LSTMBig(nn.Module):
    """Large LSTM for MNIST sequence classification (~2.4M params)."""

    def __init__(
        self, input_size: int = 28, hidden_size: int = 256, num_classes: int = 10
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size, hidden_size, num_layers=3, batch_first=True, dropout=0.2
        )
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


class Trainer:
    """Training manager with checkpoint support."""

    def __init__(self, checkpoint_dir: str = "/checkpoints") -> None:
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_path = self.checkpoint_dir / "latest.pt"
        self.model = LSTMBig()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.CrossEntropyLoss()

        self.current_epoch = 0
        self.total_epochs = int(os.environ.get("EPOCHS_TOTAL", "20"))
        self.batch_size = int(os.environ.get("BATCH_SIZE", "64"))
        self.best_accuracy = 0.0

        # Load data (suppress torchvision download progress bar)
        transform = transforms.ToTensor()
        data_root = "/runs/data"
        with (
            open(os.devnull, "w") as devnull,
            contextlib.redirect_stdout(devnull),
            contextlib.redirect_stderr(devnull),
        ):
            train_dataset = datasets.MNIST(
                root=data_root, train=True, download=True, transform=transform
            )
            test_dataset = datasets.MNIST(
                root=data_root, train=False, download=True, transform=transform
            )
        self.train_loader = DataLoader(
            train_dataset, batch_size=self.batch_size, shuffle=True, drop_last=True
        )
        self.test_loader = DataLoader(test_dataset, batch_size=256, shuffle=False)

        self.load_checkpoint()

    def load_checkpoint(self) -> None:
        if not self.checkpoint_path.exists():
            logger.info("No checkpoint found, starting from scratch")
            return
        logger.info("Loading checkpoint from %s", self.checkpoint_path)
        try:
            checkpoint = torch.load(self.checkpoint_path, weights_only=True)
            self.model.load_state_dict(checkpoint["model_state_dict"])
            self.optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
            self.current_epoch = checkpoint["epoch"]
            self.best_accuracy = checkpoint.get("best_accuracy", 0.0)
            logger.info(
                "Resumed from epoch %d (best acc: %.2f%%)",
                self.current_epoch,
                self.best_accuracy,
            )
        except Exception as e:
            logger.warning("Failed to load checkpoint, starting from scratch: %s", e)

    def save_checkpoint(self) -> None:
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint = {
            "epoch": self.current_epoch,
            "model_state_dict": self.model.state_dict(),
            "optimizer_state_dict": self.optimizer.state_dict(),
            "best_accuracy": self.best_accuracy,
        }
        fd, tmp_path = tempfile.mkstemp(dir=self.checkpoint_dir, suffix=".pt.tmp")
        try:
            torch.save(checkpoint, tmp_path)
            Path(tmp_path).replace(self.checkpoint_path)
            logger.info("Checkpoint saved at epoch %d", self.current_epoch)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise

    @torch.no_grad()
    def _evaluate(self) -> float:
        self.model.eval()
        correct = total = 0
        for images, labels in self.test_loader:
            images = images.squeeze(1)
            outputs = self.model(images)
            correct += (outputs.argmax(1) == labels).sum().item()
            total += labels.size(0)
        self.model.train()
        return 100.0 * correct / total

    def _train_one_epoch(self) -> float:
        """Run one full pass over the training dataset. Returns average loss."""
        total_loss = 0.0
        num_batches = 0
        for images, labels in self.train_loader:
            images = images.squeeze(1)
            self.optimizer.zero_grad()
            outputs = self.model(images)
            loss = self.criterion(outputs, labels)
            loss.backward()
            self.optimizer.step()
            total_loss += loss.item()
            num_batches += 1
        return total_loss / max(num_batches, 1)

    def train(self) -> None:
        logger.info(
            "Config: epochs_total=%d, batch_size=%d",
            self.total_epochs,
            self.batch_size,
        )
        logger.info(
            "Starting training from epoch %d to %d",
            self.current_epoch,
            self.total_epochs,
        )
        self.model.train()

        while self.current_epoch < self.total_epochs:
            t0 = time.monotonic()
            avg_loss = self._train_one_epoch()
            self.current_epoch += 1

            acc = self._evaluate()
            self.best_accuracy = max(self.best_accuracy, acc)
            elapsed = time.monotonic() - t0
            logger.info(
                "Epoch %d/%d - Loss: %.6f - Acc: %.2f%% - %.2fs",
                self.current_epoch,
                self.total_epochs,
                avg_loss,
                acc,
                elapsed,
            )
            self.save_checkpoint()

        logger.info("Training completed! Final epoch: %d", self.current_epoch)


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting LSTM-big training (MNIST sequences)")
    logger.info("=" * 60)
    Trainer().train()
