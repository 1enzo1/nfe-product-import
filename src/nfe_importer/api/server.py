"""FastAPI application exposing the importer pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from ..config import Settings
from ..core.pipeline import Processor


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="NF-e Product Importer")
    processor = Processor(settings)

    class ProcessRequest(BaseModel):
        files: Optional[List[str]] = None
        user: Optional[str] = None
        mode: Optional[str] = "api"

    class ReconcileRequest(BaseModel):
        sku: str
        cprod: Optional[str] = None
        barcode: Optional[str] = None
        description: Optional[str] = None
        invoice_key: Optional[str] = None
        item_number: Optional[int] = None
        user: Optional[str] = None

    def get_processor() -> Processor:
        return processor

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.post("/upload/nfe")
    async def upload_nfe(files: List[UploadFile] = File(...)) -> dict:
        saved = []
        target_folder = settings.paths.nfe_input_folder
        target_folder.mkdir(parents=True, exist_ok=True)
        for upload in files:
            destination = target_folder / upload.filename
            content = await upload.read()
            destination.write_bytes(content)
            saved.append(destination.name)
        return {"saved": saved}

    @app.post("/process")
    async def process(request: ProcessRequest, pipeline: Processor = Depends(get_processor)) -> dict:
        files: Optional[List[Path]] = None
        if request.files:
            files = []
            for filename in request.files:
                path = settings.paths.nfe_input_folder / filename
                if not path.exists():
                    raise HTTPException(status_code=404, detail=f"Arquivo {filename} não encontrado")
                files.append(path)

        if files:
            result = pipeline.process_files(files, mode=request.mode or "api", user=request.user)
        else:
            result = pipeline.process_directory(mode=request.mode or "api", user=request.user)
            if result is None:
                raise HTTPException(status_code=400, detail="Nenhum arquivo para processar")

        summary = result.summary
        return {
            "run_id": summary.run_id,
            "matched": len(summary.matched),
            "unmatched": len(summary.unmatched),
            "csv_path": str(result.dataframe_path),
            "pendings_path": str(result.pendings_path) if result.pendings_path else None,
        }

    @app.get("/runs")
    async def list_runs(pipeline: Processor = Depends(get_processor)) -> List[dict]:
        return pipeline.list_runs()

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str, pipeline: Processor = Depends(get_processor)) -> dict:
        run = pipeline.load_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Execução não encontrada")
        return run

    @app.get("/exports/{run_id}")
    async def download_csv(run_id: str, pipeline: Processor = Depends(get_processor)):
        run = pipeline.load_run(run_id)
        if not run or not run.get("csv_path"):
            raise HTTPException(status_code=404, detail="Arquivo não encontrado")
        path = Path(run["csv_path"])
        if not path.exists():
            raise HTTPException(status_code=404, detail="Arquivo não encontrado")
        return FileResponse(path, filename=path.name, media_type="text/csv")

    @app.get("/pendings/{run_id}")
    async def download_pendings(run_id: str, pipeline: Processor = Depends(get_processor)):
        run = pipeline.load_run(run_id)
        pendings_path = run.get("pendings_path") if run else None
        if not pendings_path:
            raise HTTPException(status_code=404, detail="Nenhuma pendência registrada")
        path = Path(pendings_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="Arquivo não encontrado")
        return FileResponse(path, filename=path.name, media_type="text/csv")

    @app.post("/reconcile")
    async def reconcile(request: ReconcileRequest, pipeline: Processor = Depends(get_processor)) -> dict:
        pipeline.register_manual_match(
            sku=request.sku,
            cprod=request.cprod,
            barcode=request.barcode,
            description=request.description,
            invoice_key=request.invoice_key,
            item_number=request.item_number,
            user=request.user,
        )
        return {"status": "registered"}

    @app.post("/catalog/reload")
    async def reload_catalog(pipeline: Processor = Depends(get_processor)) -> dict:
        pipeline.reload_catalog()
        return {"status": "reloaded"}

    return app


__all__ = ["create_app"]

