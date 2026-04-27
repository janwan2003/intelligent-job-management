"""Cluster infrastructure manager for the IJM backend."""

import json
import logging
import os
from pathlib import Path
from typing import Any

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

    @staticmethod
    def _resolve_path(path: Path | None, env_var: str, default: str) -> Path | None:
        """Find a config file, trying CWD then parent directory."""
        p = path or Path(os.getenv(env_var, default))
        if p.is_file():
            return p
        alt = Path("..") / p
        if alt.is_file():
            return alt
        logger.warning("Config not found at %s", p)
        return None

    def load_nodes(self, path: Path | None = None) -> None:
        config_path = self._resolve_path(path, "NODES_CONFIG", "config/nodes_config.json")
        if not config_path:
            return
        with open(config_path) as f:
            self.nodes = json.load(f)
        logger.info("Loaded %d node(s) from %s", len(self.nodes), config_path)

    def load_gpu_energy_costs(self, path: Path | None = None) -> None:
        config_path = self._resolve_path(path, "GPU_COSTS_CONFIG", "config/gpu_energy_costs.json")
        if not config_path:
            return
        with open(config_path) as f:
            self.gpu_energy_costs = json.load(f)
        logger.info("Loaded GPU energy costs for %d GPU type(s) from %s", len(self.gpu_energy_costs), config_path)

    # -- queries -------------------------------------------------------------

    def get_energy_cost(self, gpu_type: str, num_gpus: int) -> float | None:
        """Look up the hourly energy cost for *gpu_type* x *num_gpus*.

        Returns ``None`` when the combination is not in the cost table.
        """
        gpu_costs = self.gpu_energy_costs.get(gpu_type)
        if gpu_costs is None:
            return None
        return gpu_costs.get(str(num_gpus))


# Singleton instance — populated during lifespan startup
cluster: ClusterManager = ClusterManager()
