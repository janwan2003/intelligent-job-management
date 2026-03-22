#!/usr/bin/env python3
"""Sleepy job: simulates training with sleep instead of real computation.

Useful for testing the full pipeline (profiling, stop, resume, checkpointing)
without heavy CPU usage. Each "epoch" just sleeps for 1 second.
"""

import logging
import os
import random
import time
from typing import Any

import torch
import torch.nn as nn
from torch.utils.data import Dataset, TensorDataset

from base import BaseTrainer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SLEEP_PER_EPOCH = float(os.environ.get("SLEEP_PER_EPOCH", "1.0"))


class TinyModel(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.fc = nn.Linear(4, 2)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.fc(x)


class Trainer(BaseTrainer):
    def _create_model(self) -> nn.Module:
        return TinyModel()

    def _load_datasets(self) -> tuple[Dataset[Any], Dataset[Any]]:
        # Tiny synthetic dataset — 100 samples, trivial
        x = torch.randn(100, 4)
        y = (x[:, 0] > 0).long()
        ds = TensorDataset(x, y)
        return ds, ds

    def _preprocess_batch(
        self, images: torch.Tensor, labels: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return images, labels

    def _train_one_epoch(self) -> float:
        """Sleep instead of real training, then do one trivial batch."""
        jitter = random.uniform(-1.0, 3.0)
        time.sleep(max(0.1, SLEEP_PER_EPOCH + jitter))
        # One trivial forward/backward so loss is real
        loss_sum = 0.0
        n = 0
        for x, y in self.train_loader:
            x, y = self._preprocess_batch(x, y)
            self.optimizer.zero_grad()
            out = self.model(x)
            loss = self.criterion(out, y)
            loss.backward()
            self.optimizer.step()
            loss_sum += loss.item()
            n += 1
        return loss_sum / max(n, 1)


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting sleepy job (sleep-based, lightweight)")
    logger.info("=" * 60)
    Trainer().train()
