"""High level orchestration of the import pipeline."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from ..config import Settings
from .generator import CSVGenerator
from .matcher import ProductMatcher
from .models import CatalogProduct, NFEItem, ProcessingSummary
from .parser import CatalogLoader, NFEParser
from .synonyms import SynonymCache
from .utils import dump_json, now_timestamp, slugify


LOGGER = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    summary: ProcessingSummary
    dataframe_path: Path
    pendings_path: Optional[Path]


class StaticImageResolver:
    def __init__(self, settings: Settings) -> None:
        self.enabled = bool(settings.google_drive and settings.google_drive.enabled)
        self.template = settings.google_drive.public_link_template if settings.google_drive else ""
        self.mapping = self._load_mapping(settings.google_drive.mapping_file) if self.enabled else {}

    def _load_mapping(self, mapping_path: Optional[Path]) -> dict:
        if not mapping_path:
            return {}
        path = Path(mapping_path)
        if not path.exists():
            LOGGER.warning("Google Drive mapping file %s not found", path)
            return {}
        try:
            if path.suffix.lower() == ".json":
                with path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
            else:
                import pandas as pd

                df = pd.read_csv(path)
                data = {str(row["sku"]).strip(): row.get("url") or row.get("file_id") for _, row in df.iterrows()}
            return {sku: value for sku, value in data.items() if sku}
        except Exception:  # pragma: no cover - best effort loader
            LOGGER.exception("Failed to load Google Drive mapping from %s", path)
            return {}

    def __call__(self, product: CatalogProduct) -> Optional[str]:
        if not self.enabled:
            return None
        value = self.mapping.get(product.sku)
        if not value:
            return None
        if value.startswith("http"):
            return value
        return self.template.format(
            file_id=value,
            sku=product.sku,
            vendor=(product.vendor or "").strip(),
            handle=slugify(product.title or product.sku),
        )


class Processor:
    """Coordinates parsing, matching and CSV generation."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.ensure_folders()
        self.nfe_parser = NFEParser()
        self.catalog_loader = CatalogLoader(settings.paths.master_data_file)
        self.synonyms = SynonymCache(settings.paths.synonym_cache_file)
        self.catalog_products = self.catalog_loader.to_products()
        self.matcher = ProductMatcher(self.catalog_products, self.synonyms)
        self.image_resolver = StaticImageResolver(settings)
        self.generator = CSVGenerator(settings, image_resolver=self.image_resolver)

    def reload_catalog(self) -> None:
        self.catalog_products = self.catalog_loader.to_products()
        self.matcher.refresh_products(self.catalog_products)

    def process_directory(self, mode: str = "manual", user: Optional[str] = None) -> Optional[ProcessingResult]:
        xml_files = sorted(Path(self.settings.paths.nfe_input_folder).glob("*.xml"))
        if not xml_files:
            LOGGER.info("No XML files found in %s", self.settings.paths.nfe_input_folder)
            return None
        return self.process_files(xml_files, mode=mode, user=user)

    def process_files(self, files: Iterable[Path], mode: str = "manual", user: Optional[str] = None) -> ProcessingResult:
        invoices = self.nfe_parser.parse_many(files)
        items: List[NFEItem] = []
        for invoice in invoices:
            items.extend(invoice.items)

        matched, unmatched = self.matcher.match_items(items)
        run_id = now_timestamp()

        csv_path, pendings_path, dataframe, pendings_df = self.generator.generate(matched, unmatched, run_id)

        summary = ProcessingSummary(
            run_id=run_id,
            created_at=datetime.utcnow(),
            invoices=invoices,
            matched=list(matched),
            unmatched=list(unmatched),
            csv_path=csv_path,
            pendings_path=pendings_path,
            mode=mode,
            user=user,
        )

        self._persist_summary(summary, dataframe_rows=len(dataframe), pendings_rows=len(pendings_df))
        self.synonyms.save()

        return ProcessingResult(summary=summary, dataframe_path=csv_path, pendings_path=pendings_path)

    def _persist_summary(self, summary: ProcessingSummary, *, dataframe_rows: int, pendings_rows: int) -> None:
        log_folder = self.settings.paths.log_folder
        log_folder.mkdir(parents=True, exist_ok=True)
        log_path = log_folder / f"run_{summary.run_id}.json"
        payload = {
            "run_id": summary.run_id,
            "created_at": summary.created_at.isoformat(),
            "mode": summary.mode,
            "user": summary.user,
            "csv_path": str(summary.csv_path),
            "pendings_path": str(summary.pendings_path) if summary.pendings_path else None,
            "matched_count": len(summary.matched),
            "unmatched_count": len(summary.unmatched),
            "dataframe_rows": dataframe_rows,
            "pendings_rows": pendings_rows,
            "invoices": [
                {
                    "access_key": invoice.access_key,
                    "invoice_number": invoice.invoice_number,
                    "issue_date": invoice.issue_date.isoformat() if invoice.issue_date else None,
                    "supplier_name": invoice.supplier_name,
                    "supplier_cnpj": invoice.supplier_cnpj,
                    "file_path": str(invoice.file_path),
                    "items": len(invoice.items),
                }
                for invoice in summary.invoices
            ],
        }
        dump_json(log_path, payload)
        LOGGER.info(
            "Run %s: %s matched, %s unmatched. CSV: %s",
            summary.run_id,
            len(summary.matched),
            len(summary.unmatched),
            summary.csv_path,
        )

    def list_runs(self) -> List[dict]:
        runs = []
        for json_file in sorted(self.settings.paths.log_folder.glob("run_*.json")):
            data = json.loads(json_file.read_text(encoding="utf-8"))
            runs.append(data)
        runs.sort(key=lambda entry: entry.get("created_at", ""), reverse=True)
        return runs

    def load_run(self, run_id: str) -> Optional[dict]:
        log_path = self.settings.paths.log_folder / f"run_{run_id}.json"
        if not log_path.exists():
            return None
        return json.loads(log_path.read_text(encoding="utf-8"))

    def register_manual_match(
        self,
        *,
        sku: str,
        cprod: Optional[str] = None,
        barcode: Optional[str] = None,
        description: Optional[str] = None,
        invoice_key: Optional[str] = None,
        item_number: Optional[int] = None,
        user: Optional[str] = None,
    ) -> None:
        self.synonyms.register(sku=sku, cprod=cprod, barcode=barcode, description=description)
        if invoice_key and item_number is not None:
            self.synonyms.record_manual_choice(invoice_key=invoice_key, item_number=item_number, sku=sku, user=user)
        self.synonyms.save()

    def watch_folder(self, *, stop_event: Optional[threading.Event] = None) -> None:
        watch_config = self.settings.watch
        if not watch_config or not watch_config.enabled:
            LOGGER.info("Watch configuration disabled")
            return

        stop_event = stop_event or threading.Event()
        LOGGER.info("Starting watched folder service. run_at=%s interval=%s", watch_config.run_at, watch_config.interval_minutes)
        last_run_date: Optional[datetime.date] = None

        while not stop_event.is_set():
            if watch_config.run_at:
                try:
                    target_time = datetime.strptime(watch_config.run_at, "%H:%M").time()
                except ValueError:
                    LOGGER.error("Invalid run_at time format: %s", watch_config.run_at)
                    return

                now = datetime.now()
                if now.time() >= target_time and (last_run_date != now.date()):
                    self.process_directory(mode="scheduled")
                    last_run_date = now.date()
                stop_event.wait(timeout=60)
            else:
                self.process_directory(mode="watch")
                stop_event.wait(timeout=watch_config.interval_minutes * 60)


__all__ = ["Processor", "ProcessingResult"]

