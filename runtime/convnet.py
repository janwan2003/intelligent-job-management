#!/usr/bin/env python3
"""convolutionNet: 3-layer CNN image classification on CIFAR-10."""

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


class ConvNet(nn.Module):
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
        return self.classifier(x.view(x.size(0), -1))


class Trainer(BaseTrainer):
    def _create_model(self) -> nn.Module:
        return ConvNet()

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
        return images, labels  # CIFAR-10 is already (B,3,32,32)


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting convolutionNet training (CIFAR-10)")
    logger.info("=" * 60)
    Trainer().train()
