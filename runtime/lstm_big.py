#!/usr/bin/env python3
"""LSTM-big: 3-layer LSTM with dropout on MNIST (28-step sequences of 28 features)."""

import logging

import torch
import torch.nn as nn
from base import MNISTSequenceTrainer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class LSTMBig(nn.Module):
    def __init__(
        self, input_size: int = 28, hidden_size: int = 256, num_classes: int = 10
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size, hidden_size, num_layers=3, batch_first=True, dropout=0.2
        )
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # See LSTMSmall.forward — re-pack cudnn LSTM weights into a contiguous
        # buffer to avoid the per-step re-pack DataParallel forces every call.
        self.lstm.flatten_parameters()
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


class Trainer(MNISTSequenceTrainer):
    def _create_model(self) -> nn.Module:
        return LSTMBig()


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting LSTM-big training (MNIST sequences)")
    logger.info("=" * 60)
    Trainer().train()
