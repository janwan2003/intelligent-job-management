#!/usr/bin/env python3
"""
CNN image classification training — convolutional network on synthetic data.

Follows the IJM checkpoint contract:
- Loads checkpoint from /checkpoints/latest.pt on startup if it exists
- Periodically saves checkpoints during training
- Handles SIGTERM/SIGINT gracefully by checkpointing and exiting cleanly
"""

import logging
import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from types import FrameType

import torch
import torch.nn as nn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class ConvNet(nn.Module):
    """Small CNN for image classification (3-channel 32x32 input)."""

    def __init__(self, num_classes: int = 10) -> None:
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(3, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(64, 128, kernel_size=3, padding=1),
            nn.BatchNorm2d(128),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d(1),
        )
        self.classifier = nn.Linear(128, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.features(x)
        x = x.view(x.size(0), -1)
        return self.classifier(x)


class Trainer:
    """Training manager with checkpoint support."""

    def __init__(self, checkpoint_dir: str = "/checkpoints") -> None:
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_path = self.checkpoint_dir / "latest.pt"
        self.should_stop = False

        self.model = ConvNet()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.CrossEntropyLoss()

        self.current_step = 0
        self.total_steps = int(os.environ.get("MAX_STEPS", "10000"))
        self.checkpoint_interval = 200
        self.batch_size = int(os.environ.get("BATCH_SIZE", "64"))
        self.log_interval = int(os.environ.get("LOG_INTERVAL", "50"))

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        self.load_checkpoint()

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        logger.info("Received signal %s, saving checkpoint and exiting", signum)
        self.should_stop = True

    def load_checkpoint(self) -> None:
        if not self.checkpoint_path.exists():
            logger.info("No checkpoint found, starting from scratch")
            return
        logger.info("Loading checkpoint from %s", self.checkpoint_path)
        try:
            ckpt = torch.load(self.checkpoint_path, weights_only=True)
            self.model.load_state_dict(ckpt["model_state_dict"])
            self.optimizer.load_state_dict(ckpt["optimizer_state_dict"])
            self.current_step = ckpt["step"]
            logger.info("Resumed from step %d", self.current_step)
        except Exception as e:
            logger.warning("Failed to load checkpoint: %s", e)

    def save_checkpoint(self) -> None:
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        ckpt = {
            "step": self.current_step,
            "model_state_dict": self.model.state_dict(),
            "optimizer_state_dict": self.optimizer.state_dict(),
        }
        fd, tmp_path = tempfile.mkstemp(dir=self.checkpoint_dir, suffix=".pt.tmp")
        try:
            torch.save(ckpt, tmp_path)
            Path(tmp_path).replace(self.checkpoint_path)
            logger.info("Checkpoint saved at step %d", self.current_step)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise

    def train(self) -> None:
        logger.info(
            "Starting CNN training from step %d to %d",
            self.current_step,
            self.total_steps,
        )
        while self.current_step < self.total_steps:
            if self.should_stop:
                logger.info("Stopping requested, saving final checkpoint")
                self.save_checkpoint()
                sys.exit(0)

            # Synthetic batch: 3-channel 32x32 images
            batch_x = torch.randn(self.batch_size, 3, 32, 32)
            batch_y = torch.randint(0, 10, (self.batch_size,))

            self.optimizer.zero_grad()
            outputs = self.model(batch_x)
            loss = self.criterion(outputs, batch_y)
            loss.backward()
            self.optimizer.step()

            self.current_step += 1
            if self.current_step % self.log_interval == 0:
                logger.info(
                    "Step %d/%d - Loss: %.6f",
                    self.current_step,
                    self.total_steps,
                    loss.item(),
                )
            if self.current_step % self.checkpoint_interval == 0:
                self.save_checkpoint()
            time.sleep(0.1)

        logger.info("Training completed! Final step: %d", self.current_step)
        self.save_checkpoint()


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting CNN image classification training")
    logger.info("=" * 60)
    Trainer().train()
