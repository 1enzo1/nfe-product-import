import pathlib
import sys

# Ensure project src/ is importable
ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from nfe_importer.core.entrypoint import run


if __name__ == "__main__":
    data_dir = ROOT / "arena" / "data"
    out_csv = ROOT / "arena" / "variants" / "D" / "out" / "result.csv"
    # Mais flexível (balanceado). Use 0.85 se quiser mais agressivo.
    run(mode="D", data_dir=str(data_dir), out_path=str(out_csv), matcher_threshold=0.90)
