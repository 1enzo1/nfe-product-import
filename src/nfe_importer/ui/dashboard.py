"""Streamlit dashboard used to reconcile pending items."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

import pandas as pd
import streamlit as st

from ..config import Settings
from ..core.pipeline import Processor

if TYPE_CHECKING:  # pragma: no cover - only for type checkers
    from streamlit.runtime.uploaded_file_manager import UploadedFile


@st.cache_data(show_spinner=False)
def load_catalog(processor: Processor) -> pd.DataFrame:
    return processor.catalog_loader.load_dataframe()


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
        st.info("Nenhuma execução registrada até o momento.")
        return None

    run_options = {f"{run['run_id']} ({run.get('created_at', '')})": run for run in runs}
    selected = st.sidebar.selectbox("Execuções anteriores", list(run_options.keys()))
    run = run_options[selected]

    st.subheader("Resumo da execução")
    st.metric("Itens conciliados", run.get("matched_count", 0))
    st.metric("Itens pendentes", run.get("unmatched_count", 0))
    st.write(f"CSV: {run.get('csv_path')}")
    if run.get("pendings_path"):
        st.write(f"Pendências: {run.get('pendings_path')}")
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
        st.success("Sem pendências para conciliação! 🎉")
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
    suggestions = [part.split("|") for part in str(selected_row.get("suggestions", "")).split("\n") if part]
    if suggestions:
        suggestion_labels = [f"{sku.strip()} - {title.strip()} ({score.strip()})" for sku, title, score in suggestions]
        selected_suggestion = st.radio("Selecione o SKU correto", suggestion_labels)
        selected_index = suggestion_labels.index(selected_suggestion)
        chosen_sku = suggestions[selected_index][0].strip()
    else:
        st.warning("Nenhuma sugestão disponível para este item.")
        chosen_sku = st.text_input("Informe manualmente o SKU do catálogo")

    if st.button("Salvar equivalência") and chosen_sku:
        processor.register_manual_match(
            sku=chosen_sku,
            cprod=selected_row.get("cProd"),
            barcode=selected_row.get("barcode"),
            description=selected_row.get("description"),
            invoice_key=selected_row.get("invoice_key"),
            item_number=int(selected_row.get("item_number")),
            user=st.session_state.get("current_user"),
        )
        st.success("Equivalência registrada! Ela será aplicada na próxima execução.")


def show_catalog_search(processor: Processor) -> None:
    st.sidebar.header("Pesquisar no catálogo")
    catalog_df = load_catalog(processor)
    query = st.sidebar.text_input("Buscar por descrição/SKU")
    filtered = catalog_df
    if query:
        query_lower = query.lower()
        filtered = catalog_df[catalog_df.apply(lambda row: query_lower in str(row).lower(), axis=1)]
    st.sidebar.write(f"Resultados: {len(filtered)}")
    if not filtered.empty:
        st.sidebar.dataframe(filtered.head(50))


def main() -> None:
    args = parse_args()
    settings = Settings.load(args.config)
    processor = Processor(settings)

    st.set_page_config(page_title="Conciliação de NF-e", layout="wide")
    st.title("Automação de Importação de NF-e")

    st.sidebar.header("Nova execução")
    uploaded_files = st.sidebar.file_uploader("Carregar NF-e (XML)", type="xml", accept_multiple_files=True)
    current_user = st.sidebar.text_input("Usuário", value=st.session_state.get("current_user", ""))
    st.session_state["current_user"] = current_user

    if uploaded_files:
        saved_paths = save_uploaded_files(uploaded_files, settings.paths.nfe_input_folder)
        st.sidebar.success(f"{len(saved_paths)} arquivo(s) salvo(s) na pasta de entrada.")

    if st.sidebar.button("Processar agora"):
        with st.spinner("Processando arquivos..."):
            result = processor.process_directory(mode="ui", user=current_user)
        if result is None:
            st.warning("Nenhum arquivo encontrado para processamento.")
        else:
            st.success(f"Processamento concluído. CSV: {result.dataframe_path}")

    run = render_summary(processor)
    if run:
        show_pending_items(processor, run)

    show_catalog_search(processor)


if __name__ == "__main__":  # pragma: no cover
    main()

