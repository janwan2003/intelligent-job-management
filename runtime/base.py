"""Shared training infrastructure for all IJM training scripts.

Provides the Trainer base class that handles:
- Checkpoint save/load (atomic writes)
- Dataset download with suppressed progress bars
- Training loop with per-epoch logging and checkpointing
- Validation accuracy measurement

Subclasses only need to define the model, dataset, and batch preprocessing.
"""

import contextlib
import logging
import os
import tempfile
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

logger = logging.getLogger(__name__)


def download_dataset(dataset_cls: type, root: str, **kwargs: Any) -> Dataset[Any]:
    """Download a torchvision dataset with suppressed progress bars."""
    with (
        open(os.devnull, "w") as devnull,
        contextlib.redirect_stdout(devnull),
        contextlib.redirect_stderr(devnull),
    ):
        return dataset_cls(root=root, download=True, **kwargs)


class BaseTrainer(ABC):
    """Base trainer with checkpoint support and per-epoch logging.

    Subclasses must implement:
    - ``_create_model()`` — return the nn.Module
    - ``_load_datasets()`` — return (train_dataset, test_dataset)
    - ``_preprocess_batch(images, labels)`` — transform a batch before forward pass
    """

    def __init__(self, checkpoint_dir: str = "/checkpoints") -> None:
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_path = self.checkpoint_dir / "latest.pt"

        self.model = self._create_model()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.CrossEntropyLoss()

        self.current_epoch = 0
        self.total_epochs = int(os.environ.get("EPOCHS_TOTAL", "20"))
        self.batch_size = int(os.environ.get("BATCH_SIZE", "64"))
        self.best_accuracy = 0.0

        train_dataset, test_dataset = self._load_datasets()
        self.train_loader = DataLoader(
            train_dataset, batch_size=self.batch_size, shuffle=True, drop_last=True
        )
        self.test_loader = DataLoader(test_dataset, batch_size=256, shuffle=False)

        self.load_checkpoint()

    @abstractmethod
    def _create_model(self) -> nn.Module:
        """Create and return the model."""

    @abstractmethod
    def _load_datasets(self) -> tuple[Dataset[Any], Dataset[Any]]:
        """Load and return (train_dataset, test_dataset)."""

    @abstractmethod
    def _preprocess_batch(
        self, images: torch.Tensor, labels: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Preprocess a batch before the forward pass (e.g. reshape for LSTM)."""

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def load_checkpoint(self) -> None:
        """Load checkpoint if it exists."""
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
        """Save current training state atomically."""
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

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    @torch.no_grad()
    def evaluate(self) -> float:
        """Compute accuracy on the test set."""
        self.model.eval()
        correct = total = 0
        for images, labels in self.test_loader:
            images, labels = self._preprocess_batch(images, labels)
            outputs = self.model(images)
            correct += (outputs.argmax(1) == labels).sum().item()
            total += labels.size(0)
        self.model.train()
        return 100.0 * correct / total

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def _train_one_epoch(self) -> float:
        """Run one full pass over the training dataset. Returns average loss."""
        total_loss = 0.0
        num_batches = 0
        for images, labels in self.train_loader:
            images, labels = self._preprocess_batch(images, labels)
            self.optimizer.zero_grad()
            outputs = self.model(images)
            loss = self.criterion(outputs, labels)
            loss.backward()
            self.optimizer.step()
            total_loss += loss.item()
            num_batches += 1
        return total_loss / max(num_batches, 1)

    def train(self) -> None:
        """Main training loop — log and checkpoint after each epoch."""
        logger.info(
            "Config: epochs_total=%d, batch_size=%d", self.total_epochs, self.batch_size
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

            acc = self.evaluate()
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
