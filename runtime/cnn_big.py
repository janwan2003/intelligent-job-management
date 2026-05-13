#!/usr/bin/env python3
"""cnn_big: deep wide CNN on 128x128x3 synthetic tensors.

Sized so per-step compute on the *fast* A40 GPU is heavy enough that
the DataParallel split + NCCL gradient sync still wins overall.  10
conv blocks scaling 3->64->128->128->256->256->384->384->512->512,
MaxPool after every two blocks (128 -> 64 -> 32 -> 16 -> 8).  Synthetic
data so no torchvision dataset lookups (works identically on legacy
PyTorch 1.5.1 / Python 3.7 and modern 2.6 / Python 3.13).

Default batch 32 fits both nodes (P600 ~4 GB VRAM, A40 ~48 GB).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import torch
import torch.nn as nn
from base import BaseTrainer
from torch.utils.data import Dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class SyntheticTensorDataset(Dataset):
    """Random tensors + random labels, fixed-seed so two runs see the same data."""

    def __init__(
        self, n: int, channels: int = 3, size: int = 128, num_classes: int = 10
    ) -> None:
        gen = torch.Generator().manual_seed(0)
        self.x = torch.randn(n, channels, size, size, generator=gen)
        self.y = torch.randint(0, num_classes, (n,), generator=gen)

    def __len__(self) -> int:
        return self.x.shape[0]

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.x[idx], self.y[idx]


def conv_block(in_ch: int, out_ch: int) -> nn.Sequential:
    return nn.Sequential(
        nn.Conv2d(in_ch, out_ch, kernel_size=3, padding=1, bias=False),
        nn.BatchNorm2d(out_ch),
        nn.ReLU(inplace=True),
    )


class BigCNN(nn.Module):
    def __init__(self, num_classes: int = 10) -> None:
        super().__init__()
        # 128 -> 64 -> 32 -> 16 -> 8 spatial.  Two conv blocks between
        # each MaxPool, channels growing every other block so 1x1
        # bottlenecks aren't needed.
        self.body = nn.Sequential(
            conv_block(3, 64),
            conv_block(64, 64),
            nn.MaxPool2d(2),
            conv_block(64, 128),
            conv_block(128, 128),
            nn.MaxPool2d(2),
            conv_block(128, 256),
            conv_block(256, 256),
            nn.MaxPool2d(2),
            conv_block(256, 384),
            conv_block(384, 384),
            nn.MaxPool2d(2),
            conv_block(384, 512),
            conv_block(512, 512),
        )
        self.head = nn.Sequential(
            nn.AdaptiveAvgPool2d(1),
            nn.Flatten(),
            nn.Linear(512, num_classes),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.head(self.body(x))


class Trainer(BaseTrainer):
    def __init__(self, checkpoint_dir: str | None = None) -> None:
        if "BATCH_SIZE" not in os.environ:
            os.environ["BATCH_SIZE"] = "32"
        super().__init__(checkpoint_dir)

    def _create_model(self) -> nn.Module:
        return BigCNN()

    def _load_datasets(self) -> tuple[Dataset[Any], Dataset[Any]]:
        # 4096 train, 512 test — keeps the dataset small enough to live
        # entirely in RAM (~600 MB at fp32 128x128x3) while giving the
        # GPU enough batches to amortise per-epoch setup.
        n_train = int(os.environ.get("SYNTHETIC_TRAIN_N", "4096"))
        n_test = int(os.environ.get("SYNTHETIC_TEST_N", "512"))
        return SyntheticTensorDataset(n_train), SyntheticTensorDataset(n_test)

    def _preprocess_batch(
        self, images: torch.Tensor, labels: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return images, labels


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting cnn_big v2 training (synthetic 128x128x3, deep CNN)")
    logger.info("=" * 60)
    Trainer().train()
