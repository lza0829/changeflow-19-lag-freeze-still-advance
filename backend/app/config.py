import os
from pathlib import Path

APP_NAME = "Changeflow"
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
DB_FILENAME = "app.db"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def resolve_fixture_path(rel_path: str) -> Path:
    """运行期生成的夹具放在 DATA_DIR/fixtures 下并优先读取，否则回落到包内夹具。"""
    data_candidate = DATA_DIR / "fixtures" / rel_path
    if data_candidate.exists():
        return data_candidate
    return FIXTURES_DIR / rel_path
