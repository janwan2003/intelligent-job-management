"""GPU configuration utilities."""

import json


def config_key(config: dict[str, int]) -> str:
    """Canonical JSON string for a GPU config — used for set dedup & comparison."""
    return json.dumps(config, sort_keys=True)
