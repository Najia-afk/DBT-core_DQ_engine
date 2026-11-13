"""Générateur synthétique déterministe pour le MDM (config, données source, vérité, deltas)."""

from .generator import (
    generate,
    generate_day0,
    generate_delta,
    load_manifest,
    stage_live,
)
from .params import DeltaParams, SynthParams
from .plan import build_config, build_plan, write_config

__all__ = [
    "DeltaParams",
    "SynthParams",
    "build_config",
    "build_plan",
    "generate",
    "generate_day0",
    "generate_delta",
    "load_manifest",
    "stage_live",
    "write_config",
]
