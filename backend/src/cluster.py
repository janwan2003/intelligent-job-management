"""Cluster infrastructure manager for the IJM backend."""

import json
import logging
import os
from pathlib import Path
from typing import Any

from src.models import NodeConfig

logger = logging.getLogger(__name__)


class ClusterManager:
    """Mock cluster infrastructure.

    Loads node definitions from ``nodes_config.json`` and GPU energy cost
    weights from ``gpu_energy_costs.json`` at startup.  No real hardware
    interaction — everything is derived from config files.
    """

    def __init__(self) -> None:
        self.nodes: list[dict[str, Any]] = []
        self.gpu_energy_costs: dict[str, dict[str, float]] = {}

    # -- loaders -------------------------------------------------------------

    def load_nodes(self, path: Path | None = None) -> None:
        config_path = path or Path(os.getenv("NODES_CONFIG", "config/nodes_config.json"))
        if not config_path.is_file():
            # Fallback: try parent directory (when running from backend/)
            alt = Path("..") / config_path
            if alt.is_file():
                config_path = alt
            else:
                logger.warning("Nodes config not found at %s", config_path)
                return
        with open(config_path) as f:
            self.nodes = json.load(f)
        logger.info("Loaded %d node(s) from %s", len(self.nodes), config_path)

    def load_gpu_energy_costs(self, path: Path | None = None) -> None:
        config_path = path or Path(os.getenv("GPU_COSTS_CONFIG", "config/gpu_energy_costs.json"))
        if not config_path.is_file():
            alt = Path("..") / config_path
            if alt.is_file():
                config_path = alt
            else:
                logger.warning("GPU energy costs config not found at %s", config_path)
                return
        with open(config_path) as f:
            self.gpu_energy_costs = json.load(f)
        logger.info(
            "Loaded GPU energy costs for %d GPU type(s) from %s",
            len(self.gpu_energy_costs),
            config_path,
        )

    # -- queries -------------------------------------------------------------

    def get_profiling_node(self) -> NodeConfig | None:
        """Return the node designated for profiling, or ``None``."""
        for raw in self.nodes:
            node = NodeConfig.model_validate(raw)
            if node.is_for_profiling:
                return node
        return None

    def get_energy_cost(self, gpu_type: str, num_gpus: int) -> float | None:
        """Look up the hourly energy cost for *gpu_type* x *num_gpus*.

        Returns ``None`` when the combination is not in the cost table.
        """
        gpu_costs = self.gpu_energy_costs.get(gpu_type)
        if gpu_costs is None:
            return None
        return gpu_costs.get(str(num_gpus))

    def find_suitable_nodes(self, required_memory_gb: int) -> list[NodeConfig]:
        """Return nodes whose total VRAM meets *required_memory_gb*.

        Only non-profiling nodes with at least one GPU group are considered.
        """
        suitable: list[NodeConfig] = []
        for raw in self.nodes:
            node = NodeConfig.model_validate(raw)
            if node.is_for_profiling:
                continue
            if node.resources and node.get_available_memory() >= required_memory_gb:
                suitable.append(node)
        return suitable


# Singleton instance — populated during lifespan startup
cluster: ClusterManager = ClusterManager()
