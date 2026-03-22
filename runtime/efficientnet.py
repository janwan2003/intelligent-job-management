#!/usr/bin/env python3
"""efficientNet: MBConv-based image classification on CIFAR-10."""

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

CIFAR_TRANSFORM = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2470, 0.2435, 0.2616)),
    ]
)


class MBConvBlock(nn.Module):
    def __init__(
        self, in_ch: int, out_ch: int, expand_ratio: int = 4, stride: int = 1
    ) -> None:
        super().__init__()
        mid_ch = in_ch * expand_ratio
        self.use_residual = stride == 1 and in_ch == out_ch
        self.block = nn.Sequential(
            nn.Conv2d(in_ch, mid_ch, 1, bias=False),
            nn.BatchNorm2d(mid_ch),
            nn.SiLU(),
            nn.Conv2d(
                mid_ch, mid_ch, 3, stride=stride, padding=1, groups=mid_ch, bias=False
            ),
            nn.BatchNorm2d(mid_ch),
            nn.SiLU(),
            nn.Conv2d(mid_ch, out_ch, 1, bias=False),
            nn.BatchNorm2d(out_ch),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out = self.block(x)
        return out + x if self.use_residual else out


class MiniEfficientNet(nn.Module):
    def __init__(self, num_classes: int = 10) -> None:
        super().__init__()
        self.stem = nn.Sequential(
            nn.Conv2d(3, 32, 3, padding=1, bias=False), nn.BatchNorm2d(32), nn.SiLU()
        )
        self.blocks = nn.Sequential(
            MBConvBlock(32, 32),
            MBConvBlock(32, 64, stride=2),
            MBConvBlock(64, 64),
            MBConvBlock(64, 128, stride=2),
            MBConvBlock(128, 128),
        )
        self.head = nn.Sequential(
            nn.AdaptiveAvgPool2d(1), nn.Flatten(), nn.Linear(128, num_classes)
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.head(self.blocks(self.stem(x)))


class Trainer(BaseTrainer):
    def __init__(self, checkpoint_dir: str = "/checkpoints") -> None:
        # EfficientNet uses smaller default batch size
        import os

        if "BATCH_SIZE" not in os.environ:
            os.environ["BATCH_SIZE"] = "32"
        super().__init__(checkpoint_dir)

    def _create_model(self) -> nn.Module:
        return MiniEfficientNet()

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


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting efficientNet training (CIFAR-10)")
    logger.info("=" * 60)
    Trainer().train()
