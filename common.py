# -*- coding: utf-8 -*-
"""
Created on Fri Sep 26 09:02:13 2025

@author: david
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Any, Dict
import yaml

LOGGER_NAME = "industry_etl"


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        logger.setLevel(level)
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def project_paths() -> dict[str, Path]:
    root = Path.cwd()
    return {
        "root": root,
        "input": root / "input",
        "outputs": root / "outputs",
        "cache": root / "cache",
        "schema": root / "schema",
    }