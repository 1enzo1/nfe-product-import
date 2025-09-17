"""Application configuration models and helpers.

This module centralises the configuration logic of the project.  The
configuration is persisted in a YAML file (``config.yaml`` by default) and is
validated with ``pydantic`` models.  Using strongly typed configuration makes
the remaining modules easier to reason about and also gives the tests a simple
way to spin up temporary environments with customised paths.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, validator


class PathsConfig(BaseModel):
    """Filesystem locations used by the pipeline."""

    nfe_input_folder: Path = Field(..., description="Folder containing NF-e XML files")
    master_data_file: Path = Field(..., description="Excel file with the master catalogue")
    output_folder: Path = Field(..., description="Folder where generated CSV files are stored")
    log_folder: Path = Field(..., description="Folder for execution logs and run metadata")
    synonym_cache_file: Path = Field(..., description="JSON file used to persist synonym mappings")
    pendings_folder: Optional[Path] = Field(
        default=None,
        description="Optional folder where reconciliation CSV files are stored.  If not provided"
        " the output folder is used.",
    )
    temp_folder: Optional[Path] = Field(
        default=None,
        description="Folder used for temporary uploads (Streamlit/API).  Defaults to the output"
        " folder if unset.",
    )

    @validator("nfe_input_folder", "master_data_file", "output_folder", "log_folder", "synonym_cache_file", "pendings_folder", "temp_folder", pre=True)
    def _expand_path(cls, value: Optional[str]) -> Optional[Path]:
        if value is None:
            return None
        return Path(value).expanduser().resolve()

    def ensure_directories(self) -> None:
        """Create the directories required for the pipeline to operate."""

        for attr in ("nfe_input_folder", "output_folder", "log_folder"):
            path: Path = getattr(self, attr)
            path.mkdir(parents=True, exist_ok=True)

        if self.pendings_folder:
            self.pendings_folder.mkdir(parents=True, exist_ok=True)
        if self.temp_folder:
            self.temp_folder.mkdir(parents=True, exist_ok=True)


class PricingConfig(BaseModel):
    """Pricing strategy used to populate the Shopify import template."""

    strategy: str = Field("markup_fixo", description="Strategy name")
    markup_factor: float = Field(2.2, description="Factor applied when using markup_fixo")
    currency: str = Field("BRL", description="Currency used for generated prices")

    @validator("strategy")
    def _validate_strategy(cls, value: str) -> str:
        allowed = {"markup_fixo", "tabela", "somente_custo"}
        if value not in allowed:
            raise ValueError(f"Unsupported pricing strategy '{value}'. Valid values: {sorted(allowed)}")
        return value

    @validator("markup_factor")
    def _validate_factor(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("markup_factor must be greater than zero")
        return value


class CSVOutputConfig(BaseModel):
    """Information about the generated CSV file."""

    filename_prefix: str = Field("importacao_produtos_", description="Prefix applied to generated CSV files")
    columns: List[str] = Field(..., description="Order of columns in the output CSV")
    delimiter: str = Field(",", description="Delimiter used when writing the CSV")


class MetafieldsConfig(BaseModel):
    namespace: str = Field("custom", description="Shopify metafield namespace")
    keys: Dict[str, str] = Field(default_factory=dict, description="Mapping of logical names to metafield keys")


class GoogleDriveConfig(BaseModel):
    """Optional Google Drive integration configuration."""

    enabled: bool = Field(False, description="If true the pipeline will try to resolve image URLs from Google Drive")
    credentials_file: Optional[Path] = Field(default=None, description="Path to a service account JSON file")
    mapping_file: Optional[Path] = Field(
        default=None,
        description="Optional CSV/JSON mapping between catalogue SKUs and Google Drive file identifiers",
    )
    public_link_template: str = Field(
        "https://drive.google.com/uc?id={file_id}",
        description="Template used to build a public link for an image.  ``{file_id}`` will be replaced",
    )
    folder_pattern: str = Field(
        "{vendor}/{sku}",
        description="Pattern used when looking up images programmatically.  The pattern may use"
        " placeholders present in :class:`CatalogProduct` such as ``{vendor}``, ``{sku}`` or ``{handle}``.",
    )

    @validator("credentials_file", "mapping_file", pre=True)
    def _expand_optional_path(cls, value: Optional[str]) -> Optional[Path]:
        if value is None:
            return None
        return Path(value).expanduser().resolve()


class WatchConfig(BaseModel):
    """Configuration for the background scheduler/watch service."""

    enabled: bool = Field(False, description="Enable the watched folder scheduler")
    run_at: Optional[str] = Field(
        default=None,
        description="Time of day in HH:MM 24h format when the pipeline should run.  When omitted,"
        " the watch service will poll the folder every ``interval_minutes``.",
    )
    interval_minutes: int = Field(15, description="Polling interval in minutes when ``run_at`` is not defined")

    @validator("interval_minutes")
    def _validate_interval(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("interval_minutes must be greater than zero")
        return value


class Settings(BaseModel):
    """Top level configuration object."""

    paths: PathsConfig
    pricing: PricingConfig = Field(default_factory=PricingConfig)
    csv_output: CSVOutputConfig
    metafields: MetafieldsConfig = Field(default_factory=MetafieldsConfig)
    google_drive: Optional[GoogleDriveConfig] = None
    watch: Optional[WatchConfig] = None
    default_vendor: Optional[str] = Field(
        default=None,
        description="Default vendor name used when the catalogue does not provide one",
    )

    class Config:
        arbitrary_types_allowed = True

    def ensure_folders(self) -> None:
        """Create all folders referenced by the configuration."""

        self.paths.ensure_directories()

    @classmethod
    def load(cls, path: Path | str = Path("config.yaml")) -> "Settings":
        """Load the configuration from a YAML file."""

        config_path = Path(path).expanduser().resolve()
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with config_path.open("r", encoding="utf-8") as stream:
            data = yaml.safe_load(stream) or {}

        settings = cls.parse_obj(data)
        settings.ensure_folders()
        return settings


__all__ = [
    "Settings",
    "PathsConfig",
    "PricingConfig",
    "CSVOutputConfig",
    "MetafieldsConfig",
    "GoogleDriveConfig",
    "WatchConfig",
]

