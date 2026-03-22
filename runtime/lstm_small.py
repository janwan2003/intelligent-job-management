#!/usr/bin/env python3
"""LSTM-small: 1-layer LSTM on MNIST (28-step sequences of 28 features)."""

import logging
from typing import Any

import torch
import torch.nn as nn
from torch.utils.data import Dataset
from torchvision import datasets, transforms

from base import BaseTrainer, download_dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_ROOT = "/runs/data"


class LSTMSmall(nn.Module):
    def __init__(
        self, input_size: int = 28, hidden_size: int = 128, num_classes: int = 10
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers=1, batch_first=True)
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


class Trainer(BaseTrainer):
    def _create_model(self) -> nn.Module:
        return LSTMSmall()

    def _load_datasets(self) -> tuple[Dataset[Any], Dataset[Any]]:
        t = transforms.ToTensor()
        return (
            download_dataset(datasets.MNIST, DATA_ROOT, train=True, transform=t),
            download_dataset(datasets.MNIST, DATA_ROOT, train=False, transform=t),
        )

    def _preprocess_batch(
        self, images: torch.Tensor, labels: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return images.squeeze(1), labels  # (B,1,28,28) -> (B,28,28)


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting LSTM-small training (MNIST sequences)")
    logger.info("=" * 60)
    Trainer().train()
