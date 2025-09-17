"""Command line interface for the NF-e product importer."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

from .config import Settings
from .core.pipeline import Processor


LOGGER = logging.getLogger(__name__)


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def load_settings(config_path: str) -> Settings:
    return Settings.load(config_path)


def command_process(args: argparse.Namespace) -> None:
    settings = load_settings(args.config)
    processor = Processor(settings)
    files = [Path(file) for file in args.files] if args.files else None

    if files:
        result = processor.process_files(files, mode=args.mode, user=args.user)
    else:
        result = processor.process_directory(mode=args.mode, user=args.user)
        if result is None:
            print("Nenhum arquivo encontrado para processamento.")
            return

    summary = result.summary
    print("Processamento concluído")
    print(f"Run ID: {summary.run_id}")
    print(f"Itens conciliados: {len(summary.matched)}")
    print(f"Itens pendentes: {len(summary.unmatched)}")
    print(f"CSV gerado: {result.dataframe_path}")
    if result.pendings_path:
        print(f"Pendências: {result.pendings_path}")


def command_watch(args: argparse.Namespace) -> None:
    settings = load_settings(args.config)
    processor = Processor(settings)
    processor.watch_folder()


def command_api(args: argparse.Namespace) -> None:
    from .api.server import create_app

    settings = load_settings(args.config)
    app = create_app(settings)
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)


def command_ui(args: argparse.Namespace) -> None:
    settings_path = Path(args.config).expanduser().resolve()
    dashboard_path = Path(__file__).resolve().parent / "ui" / "dashboard.py"
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(dashboard_path),
        "--",
        "--config",
        str(settings_path),
    ]
    subprocess.run(cmd, check=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NF-e Product Importer")
    parser.add_argument("--config", default="config.yaml", help="Path to the configuration YAML file")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    subparsers = parser.add_subparsers(dest="command", required=True)

    process_parser = subparsers.add_parser("process", help="Process NF-e files")
    process_parser.add_argument("--config", default="config.yaml")
    process_parser.add_argument("--mode", default="manual", help="Execution mode label")
    process_parser.add_argument("--user", default=None, help="User responsible for the execution")
    process_parser.add_argument("files", nargs="*", help="Specific XML files to process")
    process_parser.set_defaults(func=command_process)

    watch_parser = subparsers.add_parser("watch", help="Start the watched folder service")
    watch_parser.add_argument("--config", default="config.yaml")
    watch_parser.set_defaults(func=command_watch)

    api_parser = subparsers.add_parser("api", help="Start the FastAPI server")
    api_parser.add_argument("--config", default="config.yaml")
    api_parser.add_argument("--host", default="0.0.0.0")
    api_parser.add_argument("--port", type=int, default=8000)
    api_parser.add_argument("--reload", action="store_true", help="Enable auto reload (development only)")
    api_parser.set_defaults(func=command_api)

    ui_parser = subparsers.add_parser("ui", help="Launch the reconciliation dashboard")
    ui_parser.add_argument("--config", default="config.yaml")
    ui_parser.set_defaults(func=command_ui)

    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    configure_logging(args.verbose)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()

