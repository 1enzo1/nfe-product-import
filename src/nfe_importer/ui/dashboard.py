"""Streamlit dashboard used to reconcile pending items.

Notes for Streamlit Cloud deployment:
- This app expects the project package to be importable as ``nfe_importer``.
- When running on Streamlit Cloud (streamlit.io), ensure that ``src/`` is on
  ``PYTHONPATH``. We also add it programmatically below for robustness.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

import pandas as pd
import streamlit as st

# Make sure the package is importable when running via "streamlit run"
import sys
# Path layout: <repo>/src/nfe_importer/ui/dashboard.py
# parents[0]=ui, [1]=nfe_importer, [2]=src, [3]=repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC_PATH = _REPO_ROOT / "src"
if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

from nfe_importer.config import Settings
from nfe_importer.core.pipeline import Processor
from nfe_importer.core.parser import CatalogLoader

if TYPE_CHECKING:  # pragma: no cover - only for type checkers
    from streamlit.runtime.uploaded_file_manager import UploadedFile


@st.cache_data(show_spinner=False)
def load_catalog_from_file(excel_path: str):
    loader = CatalogLoader(Path(excel_path))
    return loader.load_dataframe()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args, _ = parser.parse_known_args()
    return args


def save_uploaded_files(files: List["UploadedFile"], target_folder: Path) -> List[Path]:
    saved_paths = []
    target_folder.mkdir(parents=True, exist_ok=True)
    for uploaded in files:
        destination = target_folder / uploaded.name
        destination.write_bytes(uploaded.getbuffer())
        saved_paths.append(destination)
    return saved_paths


def render_summary(processor: Processor) -> Optional[dict]:
    runs = processor.list_runs()
    if not runs:
        st.info("Nenhuma execuÃ§Ã£o registrada atÃ© o momento.")
        return None

    run_options = {f"{run['run_id']} ({run.get('created_at', '')})": run for run in runs}
    selected = st.sidebar.selectbox("ExecuÃ§Ãµes anteriores", list(run_options.keys()))
    run = run_options[selected]

    st.subheader("Resumo da execuÃ§Ã£o")
    st.metric("Itens conciliados", run.get("matched_count", 0))
    st.metric("Itens pendentes", run.get("unmatched_count", 0))
    st.write(f"CSV: {run.get('csv_path')}")
    if run.get("pendings_path"):
        st.write(f"PendÃªncias: {run.get('pendings_path')}")
    return run


def load_pendings(run: dict) -> pd.DataFrame:
    pendings_path = run.get("pendings_path")
    if not pendings_path:
        return pd.DataFrame()
    path = Path(pendings_path)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def show_pending_items(processor: Processor, run: dict) -> None:
    pendings_df = load_pendings(run)
    if pendings_df.empty:
        st.success("Sem pendÃªncias para conciliaÃ§Ã£o! ðŸŽ‰")
        return

    st.subheader("Itens pendentes")
    st.dataframe(pendings_df)

    item_options = {
        f"NF {row.invoice_key} - Item {row.item_number} - {row.description[:50]}": idx
        for idx, row in pendings_df.iterrows()
    }
    selected_key = st.selectbox("Selecione um item para conciliar", list(item_options.keys()))
    selected_row = pendings_df.iloc[item_options[selected_key]]

    st.markdown("### Sugestões do catálogo")
    suggestions_raw = str(selected_row.get("suggestions", ""))
    suggestions = [part.split("|") for part in suggestions_raw.splitlines() if part]
    if suggestions:
        suggestion_labels = [f"{sku.strip()} - {title.strip()} ({score.strip()})" for sku, title, score in suggestions]
        selected_suggestion = st.radio("Selecione o SKU correto", suggestion_labels)
        selected_index = suggestion_labels.index(selected_suggestion)
        chosen_sku = suggestions[selected_index][0].strip()
    else:
        st.warning("Nenhuma sugestÃ£o disponÃ­vel para este item.")
        chosen_sku = st.text_input("Informe manualmente o SKU do catÃ¡logo")

    if st.button("Salvar equivalÃªncia") and chosen_sku:
        processor.register_manual_match(
            sku=chosen_sku,
            cprod=selected_row.get("cProd"),
            barcode=selected_row.get("barcode"),
            description=selected_row.get("description"),
            invoice_key=selected_row.get("invoice_key"),
            item_number=int(selected_row.get("item_number")),
            user=st.session_state.get("current_user"),
        )
        st.success("EquivalÃªncia registrada! Ela serÃ¡ aplicada na prÃ³xima execuÃ§Ã£o.")


def show_catalog_search(processor: Processor) -> None:
    st.sidebar.header("Pesquisar no catÃ¡logo")
    excel_path = str(processor.catalog_loader.excel_path)
    catalog_df = load_catalog_from_file(excel_path)
    query = st.sidebar.text_input("Buscar por descriÃ§Ã£o/SKU")
    filtered = catalog_df
    if query:
        query_lower = query.lower()
        filtered = catalog_df[catalog_df.apply(lambda row: query_lower in str(row).lower(), axis=1)]
    st.sidebar.write(f"Resultados: {len(filtered)}")
    if not filtered.empty:
        st.sidebar.dataframe(filtered.head(50))


def _load_settings_with_fallback(config_path: str) -> Settings:
    """Load settings; if master Excel is missing, fallback to example_docs/.

    This improves UX on Streamlit Cloud, where only the repository files are
    available and a local data/ folder may be empty.
    """
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        cfg_path = Path("config.yaml")
    settings = Settings.load(str(cfg_path))
    # If the configured master_data_file doesn't exist, try to discover one
    mfile = Path(settings.paths.master_data_file)
    if not mfile.exists():
        candidates = list((Path.cwd() / "example_docs").glob("*.xlsx"))
        if candidates:
            settings.paths.master_data_file = candidates[0]
            settings.ensure_folders()
    return settings


def _discover_versions(default_config: str) -> list[tuple[str, Path]]:
    """Discover available versioned configs.

    Returns a list of (label, path) where the first item is always the
    current default config.
    """
    repo_root = _REPO_ROOT
    versions: list[tuple[str, Path]] = [("Padrão (config.yaml)", Path(default_config).resolve())]

    # pipelines/<name>/config.yaml or config_<name>.yaml
    pipelines_dir = repo_root / "pipelines"
    if pipelines_dir.exists():
        for sub in pipelines_dir.iterdir():
            if not sub.is_dir():
                continue
            cfg = sub / "config.yaml"
            if not cfg.exists():
                alt = sub / f"config_{sub.name}.yaml"
                cfg = alt if alt.exists() else cfg
            if cfg.exists():
                label = sub.name.replace("_", " ").replace("-", " ")
                versions.append((label, cfg.resolve()))

    # Root-level config.<name>.yaml
    for cfg in repo_root.glob("config.*.yaml"):
        label = cfg.stem.split(".", 1)[1].replace("_", " ").replace("-", " ")
        versions.append((label, cfg.resolve()))

    # Deduplicate by absolute path preserving order
    dedup: list[tuple[str, Path]] = []
    seen = set()
    for label, path in versions:
        ap = str(path)
        if ap in seen:
            continue
        seen.add(ap)
        dedup.append((label, path))
    return dedup


def main() -> None:
    args = parse_args()
    versions = _discover_versions(args.config)
    st.sidebar.header("Versão do processamento")
    version_labels = [label for label, _ in versions]
    selected_label = st.sidebar.selectbox("Selecione a versão", version_labels, index=0)
    selected_path = next(path for label, path in versions if label == selected_label)

    settings = _load_settings_with_fallback(str(selected_path))
    processor = Processor(settings)

    st.set_page_config(page_title="Conciliação de NF-e", layout="wide")
    st.title("Automação de Importação de NF-e")

    st.sidebar.header("Nova execução")
    uploaded_files = st.sidebar.file_uploader("Carregar NF-e (XML)", type="xml", accept_multiple_files=True)
    st.sidebar.caption(f"Config: {selected_path}")
    current_user = st.sidebar.text_input("UsuÃ¡rio", value=st.session_state.get("current_user", ""))
    st.session_state["current_user"] = current_user

    if uploaded_files:
        saved_paths = save_uploaded_files(uploaded_files, settings.paths.nfe_input_folder)
        st.sidebar.success(f"{len(saved_paths)} arquivo(s) carregado(s).")

    if st.sidebar.button("Processar agora"):
        with st.spinner("Processando arquivos..."):
            result = processor.process_directory(mode=f"ui:{selected_label}", user=current_user)
        if result is None:
            st.warning("Nenhum arquivo encontrado para processamento.")
        else:
            st.success("Processamento concluído.")

            try:
                from pathlib import Path as _P
                _csv = _P(str(result.dataframe_path))
                if _csv.exists():
                    st.download_button("Baixar CSV gerado", data=_csv.read_bytes(), file_name=_csv.name, mime="text/csv")
            except Exception:
                pass
            try:
                if result.pendings_path:
                    _pend = _P(str(result.pendings_path))
                    if _pend.exists():
                        st.download_button("Baixar Pendências", data=_pend.read_bytes(), file_name=_pend.name, mime="text/csv")
            except Exception:
                pass
    run = render_summary(processor)
    if run:
        show_pending_items(processor, run)

    show_catalog_search(processor)


if __name__ == "__main__":  # pragma: no cover
    main()











