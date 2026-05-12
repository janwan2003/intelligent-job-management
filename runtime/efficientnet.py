#!/usr/bin/env python3
"""efficientNet: MBConv-based image classification on CIFAR-10."""

from __future__ import annotations

import logging
import os

import torch
import torch.nn as nn
from base import CIFAR10Trainer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


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


class Trainer(CIFAR10Trainer):
    def __init__(self, checkpoint_dir: str | None = None) -> None:
        # EfficientNet trains with a smaller default batch size; setting the
        # env var BEFORE super().__init__() reads it is intentional.
        if "BATCH_SIZE" not in os.environ:
            os.environ["BATCH_SIZE"] = "32"
        super().__init__(checkpoint_dir)

    def _create_model(self) -> nn.Module:
        return MiniEfficientNet()


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting efficientNet training (CIFAR-10)")
    logger.info("=" * 60)
    Trainer().train()
