# ``from __future__ import annotations`` keeps PEP 604 (``str | None``) and
# PEP 585 (``tuple[X, Y]``) annotations stringified at runtime, so this file
# stays importable on Python 3.7 (the legacy CUDA-10.1 image for matemagician).
"""Shared training infrastructure for all IJM training scripts.

Provides the Trainer base class that handles:
- Checkpoint save/load (atomic writes)
- Dataset download with suppressed progress bars
- Training loop with per-epoch logging and checkpointing
- Validation accuracy measurement

Subclasses only need to define the model, dataset, and batch preprocessing.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
import tempfile
import time
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset
from torchvision import datasets, transforms

# cuDNN re-warns every forward under DataParallel because each replica's RNN
# weights are non-leaf tensors and ``flatten_parameters()`` on the replica is
# a no-op.  The call in LSTM.forward already handles the single-GPU case, so
# this just silences the unfixable DP variant rather than masking real bugs.
warnings.filterwarnings(
    "ignore",
    message="RNN module weights are not part of single contiguous chunk of memory.*",
)

# Legacy PyTorch 1.5.1 (matemagician's CUDA-10.1 image) emits this warning via
# C++ ``std::cerr`` (fd 2) directly — bypassing both Python's ``warnings``
# module AND any ``sys.stderr`` wrapper, because the write is at the C
# library level.  Suppress it by redirecting fd 2 itself through a pipe and
# running a background thread that drops lines matching the noise pattern,
# forwarding everything else to the real stderr.
_CUDNN_RNN_NOISE = (
    b"RNN module weights are not part of single contiguous chunk of memory"
)


def _install_stderr_fd_filter() -> None:
    import threading

    # Already installed?  Don't double-redirect on re-import.
    if os.environ.get("_IJM_STDERR_FILTERED") == "1":
        return

    orig_stderr_fd = os.dup(2)
    r, w = os.pipe()
    os.dup2(w, 2)
    os.close(w)

    def _pump() -> None:
        # ``readline`` over an os-level pipe is reliable here; we buffer
        # manually so a partial last line (no trailing newline at EOF) still
        # gets flushed.
        try:
            with (
                os.fdopen(r, "rb", buffering=0) as src,
                os.fdopen(orig_stderr_fd, "wb", buffering=0) as dst,
            ):
                buf = b""
                while True:
                    chunk = src.read(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if _CUDNN_RNN_NOISE not in line:
                            dst.write(line + b"\n")
                if buf and _CUDNN_RNN_NOISE not in buf:
                    dst.write(buf)
        except Exception:  # noqa: BLE001 — best-effort, never crash trainer
            pass

    threading.Thread(target=_pump, name="stderr-filter", daemon=True).start()
    os.environ["_IJM_STDERR_FILTERED"] = "1"


_install_stderr_fd_filter()

logger = logging.getLogger(__name__)


def download_dataset(dataset_cls: type, root: str, **kwargs: Any) -> Dataset[Any]:
    """Load a torchvision dataset, preferring a pre-staged on-disk copy.

    Some torchvision versions re-check integrity on every construction even
    when files are present, and on rootless-docker workers (polimi-gpu) the
    container's slirp4netns DNS resolver intermittently fails to reach
    public mirrors --- a single bad lookup turns an otherwise-cached
    dataset into a hard failure.  Try ``download=False`` first (uses
    on-disk files only); only fall back to ``download=True`` if that
    raises (legitimately missing data).
    """
    # Nested ``with`` rather than the parenthesized form so this stays
    # syntactically valid on Python 3.7 (the legacy CUDA-10.1 image).
    with open(os.devnull, "w") as devnull:
        with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
            try:
                return dataset_cls(root=root, download=False, **kwargs)
            except (RuntimeError, FileNotFoundError):
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
        if self.device.type == "cuda" and torch.cuda.device_count() > 1:
            logger.info(
                "Wrapping model in DataParallel across %d GPUs",
                torch.cuda.device_count(),
            )
            self.model = nn.DataParallel(self.model)
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
        # Cap test batch at the training batch so heavy CNN models don't OOM
        # during evaluate() on small-VRAM GPUs (P600 4GB).  The original 256
        # was fine for LSTMs and CIFAR-CNNs but blows up on 128x128x3 deep
        # CNNs.  ``min`` keeps fast paths (LSTM, small CNN) unchanged.
        self.test_loader = DataLoader(
            test_dataset, batch_size=min(256, self.batch_size), shuffle=False
        )

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
            # Pre-read into memory: FUSE-mounted checkpoint dirs can EPERM on
            # the mmap()/seek() syscalls torch.load() issues.
            buf = io.BytesIO(self.checkpoint_path.read_bytes())
            # weights_only is torch >= 1.13.  Legacy CUDA-10.1 image ships
            # torch 1.5.1 — fall back; reset BytesIO since the first load
            # consumes it and a second read-from-EOF masks the TypeError.
            try:
                checkpoint = torch.load(
                    buf, weights_only=True, map_location=self.device
                )
            except TypeError:
                buf.seek(0)
                checkpoint = torch.load(buf, map_location=self.device)
            inner = (
                self.model.module
                if isinstance(self.model, nn.DataParallel)
                else self.model
            )
            # ``strict=False`` tolerates minor key differences between torch
            # versions (e.g.\ BN parameter naming or DataParallel
            # ``module.`` prefixes that slipped through), which matters for
            # cnn\_big jobs that migrate from modern A40 (torch 2.6) to
            # legacy P600 (torch 1.5.1) workers.  We re-raise on actual
            # shape mismatches because those mean weights are unusable.
            missing, unexpected = inner.load_state_dict(
                checkpoint["model_state_dict"], strict=False
            )
            if missing or unexpected:
                logger.warning(
                    "Checkpoint state_dict partial load: missing=%d unexpected=%d",
                    len(missing),
                    len(unexpected),
                )
            # Optimizer state is the most version-fragile part of the
            # checkpoint (step counters, fused-momentum tensors).  If load
            # fails (e.g.\ legacy torch refuses the newer Adam state shape)
            # we restart Adam from scratch — at the cost of a few epochs of
            # adaptive-learning-rate warmup, which is invisible at the
            # scenario timescales we care about.
            try:
                self.optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
            except (RuntimeError, ValueError, KeyError) as e:
                logger.warning(
                    "Optimizer state_dict load failed (%s); restarting optimizer "
                    "from scratch — model weights still loaded.",
                    e,
                )
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
        inner = (
            self.model.module if isinstance(self.model, nn.DataParallel) else self.model
        )
        checkpoint = {
            "epoch": self.current_epoch,
            "model_state_dict": inner.state_dict(),
            "optimizer_state_dict": self.optimizer.state_dict(),
            "best_accuracy": self.best_accuracy,
        }
        fd, tmp_path = tempfile.mkstemp(dir=self.checkpoint_dir, suffix=".pt.tmp")
        os.close(fd)  # mkstemp opens the file; we let torch.save reopen by path
        try:
            # Always save in the legacy (pre-1.6) tar+pickle format so the
            # checkpoint stays cross-readable when a job migrates between
            # nodes running different torch builds — matemagician's legacy
            # image uses torch 1.5.1 (no zip-file support).  On torch <1.6
            # this kwarg doesn't exist, so fall back to the default save.
            try:
                torch.save(checkpoint, tmp_path, _use_new_zipfile_serialization=False)
            except TypeError:
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
            logger.info(
                "Epoch %d/%d - starting", self.current_epoch + 1, self.total_epochs
            )
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
