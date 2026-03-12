#!/usr/bin/env python3
"""
Stoppable/resumable training script with checkpoint support.

This script demonstrates a training loop that:
- Loads checkpoint from /checkpoints/latest.pt on startup if it exists
- Periodically saves checkpoints during training
- Handles SIGTERM/SIGINT gracefully by checkpointing and exiting cleanly
- Logs progress to stdout for monitoring
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class SimpleModel(nn.Module):
    """Simple neural network for demonstration."""

    def __init__(
        self, input_size: int = 100, hidden_size: int = 50, output_size: int = 10
    ) -> None:
        super().__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        return x


class Trainer:
    """Training manager with checkpoint support."""

    def __init__(self, checkpoint_dir: str = "/checkpoints") -> None:
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_path = self.checkpoint_dir / "latest.pt"
        self.should_stop = False

        # Model and optimizer
        self.model = SimpleModel()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.MSELoss()

        # Training state
        self.current_step = 0
        self.total_steps = int(os.environ.get("MAX_STEPS", "10000"))
        self.checkpoint_interval = 200

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Load checkpoint if exists
        self.load_checkpoint()

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        """Handle SIGTERM/SIGINT by checkpointing and stopping."""
        logger.info("Received signal %s, saving checkpoint and exiting", signum)
        self.should_stop = True

    def load_checkpoint(self) -> None:
        """Load checkpoint if it exists."""
        if not self.checkpoint_path.exists():
            logger.info("No checkpoint found, starting from scratch")
            return

        logger.info("Loading checkpoint from %s", self.checkpoint_path)
        try:
            checkpoint = torch.load(self.checkpoint_path, weights_only=True)
            for key in ("model_state_dict", "optimizer_state_dict", "step"):
                if key not in checkpoint:
                    logger.warning(
                        "Checkpoint missing key '%s', starting from scratch", key
                    )
                    return
            self.model.load_state_dict(checkpoint["model_state_dict"])
            self.optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
            self.current_step = checkpoint["step"]
            logger.info("Resumed from step %d", self.current_step)
        except Exception as e:
            logger.warning("Failed to load checkpoint, starting from scratch: %s", e)

    def save_checkpoint(self) -> None:
        """Save current training state atomically."""
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint = {
            "step": self.current_step,
            "model_state_dict": self.model.state_dict(),
            "optimizer_state_dict": self.optimizer.state_dict(),
        }
        # Write to temp file then atomically rename to prevent corruption
        fd, tmp_path = tempfile.mkstemp(dir=self.checkpoint_dir, suffix=".pt.tmp")
        try:
            torch.save(checkpoint, tmp_path)
            Path(tmp_path).replace(self.checkpoint_path)
            logger.info("Checkpoint saved at step %d", self.current_step)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise

    def train(self) -> None:
        """Main training loop."""
        logger.info(
            "Starting training from step %d to %d", self.current_step, self.total_steps
        )

        while self.current_step < self.total_steps:
            if self.should_stop:
                logger.info("Stopping requested, saving final checkpoint")
                self.save_checkpoint()
                logger.info("Exiting cleanly")
                sys.exit(0)

            # Simulate training step
            batch_x = torch.randn(32, 100)
            batch_y = torch.randn(32, 10)

            self.optimizer.zero_grad()
            outputs = self.model(batch_x)
            loss = self.criterion(outputs, batch_y)
            loss.backward()
            self.optimizer.step()

            self.current_step += 1

            # Print progress
            if self.current_step % 50 == 0:
                logger.info(
                    "Step %d/%d - Loss: %.6f",
                    self.current_step,
                    self.total_steps,
                    loss.item(),
                )

            # Periodic checkpoint
            if self.current_step % self.checkpoint_interval == 0:
                self.save_checkpoint()

            # Simulate computation time
            time.sleep(0.1)

        # Training complete
        logger.info("Training completed! Final step: %d", self.current_step)
        self.save_checkpoint()
        logger.info("Exiting successfully")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting training job")
    logger.info("=" * 60)

    trainer = Trainer()
    trainer.train()
