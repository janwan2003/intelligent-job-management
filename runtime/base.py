"""Shared training infrastructure for all IJM training scripts.

Provides the Trainer base class that handles:
- Checkpoint save/load (atomic writes)
- Dataset download with suppressed progress bars
- Training loop with per-epoch logging and checkpointing
- Validation accuracy measurement

Subclasses only need to define the model, dataset, and batch preprocessing.
"""

import contextlib
import io
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
from torchvision import datasets, transforms

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

    def __init__(self, checkpoint_dir: str | None = None) -> None:
        # Match EPOCHS_TOTAL / BATCH_SIZE: env-driven by default, with the
        # constructor argument as an override (used by tests).
        self.checkpoint_dir = Path(
            checkpoint_dir or os.environ.get("CHECKPOINT_DIR", "/checkpoints")
        )
        self.checkpoint_path = self.checkpoint_dir / "latest.pt"

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info("Using device: %s", self.device)

        self.model = self._create_model().to(self.device)
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
            # Pre-read into memory before torch.load.  When the checkpoint dir
            # is on a FUSE mount (rclone/sshfs for cross-node sharing), the
            # mmap()/seek() syscalls torch.load() may issue can fail with
            # EPERM ("Operation not permitted").  Reading via plain read()
            # avoids those calls entirely and is cheap for our checkpoint sizes.
            buf = io.BytesIO(self.checkpoint_path.read_bytes())
            checkpoint = torch.load(buf, weights_only=True, map_location=self.device)
            self.model.load_state_dict(checkpoint["model_state_dict"])
            self.optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
            self.current_epoch = checkpoint["epoch"]
            self.best_accuracy = checkpoint.get("best_accuracy", 0.0)
            logger.info(
                "Resumed from epoch %d (best acc: %.2f%%)",
                self.current_epoch,
                self.best_accuracy,
            )
        except (OSError, RuntimeError, KeyError, EOFError) as e:
            # OSError covers FUSE/filesystem errors; RuntimeError covers
            # torch.load failures (corrupted file, version mismatch);
            # KeyError covers missing keys in the checkpoint dict.  Anything
            # else (e.g. CUDA OOM during load) should bubble up — we don't
            # want to start from scratch on a transient hardware issue.
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
        os.close(fd)  # mkstemp opens the file; we let torch.save reopen by path
        try:
            torch.save(checkpoint, tmp_path)
            Path(tmp_path).replace(self.checkpoint_path)
            logger.info("Checkpoint saved at epoch %d", self.current_epoch)
        except Exception:
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
            images = images.to(self.device, non_blocking=True)
            labels = labels.to(self.device, non_blocking=True)
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
            images = images.to(self.device, non_blocking=True)
            labels = labels.to(self.device, non_blocking=True)
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


# ---------------------------------------------------------------------------
# Dataset mixins — let concrete trainers focus on the model, not the loader
# ---------------------------------------------------------------------------

DATA_ROOT = "/runs/data"

CIFAR_TRANSFORM = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2470, 0.2435, 0.2616)),
    ]
)


class MNISTTrainer(BaseTrainer):
    """Trainer pre-wired for MNIST; identity batch preprocessing by default."""

    def _load_datasets(self) -> tuple[Dataset[Any], Dataset[Any]]:
        t = transforms.ToTensor()
        return (
            download_dataset(datasets.MNIST, DATA_ROOT, train=True, transform=t),
            download_dataset(datasets.MNIST, DATA_ROOT, train=False, transform=t),
        )

    def _preprocess_batch(
        self, images: torch.Tensor, labels: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return images, labels


class MNISTSequenceTrainer(MNISTTrainer):
    """MNIST trainer that reshapes batches into 28×28 sequences for LSTMs."""

    def _preprocess_batch(
        self, images: torch.Tensor, labels: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return images.squeeze(1), labels  # (B,1,28,28) -> (B,28,28)


class CIFAR10Trainer(BaseTrainer):
    """Trainer pre-wired for CIFAR-10 with the canonical normalisation."""

    def _load_datasets(self) -> tuple[Dataset[Any], Dataset[Any]]:
        return (
            download_dataset(
                datasets.CIFAR10, DATA_ROOT, train=True, transform=CIFAR_TRANSFORM
            ),
            download_dataset(
                datasets.CIFAR10, DATA_ROOT, train=False, transform=CIFAR_TRANSFORM
            ),
        )

    def _preprocess_batch(
        self, images: torch.Tensor, labels: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return images, labels
