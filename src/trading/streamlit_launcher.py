from __future__ import annotations

import sys
from pathlib import Path

from trading.logging_config import configure_logging


def main() -> None:
    from streamlit.web import cli as streamlit_cli

    configure_logging(level="INFO", force=False)
    app_path = Path(__file__).with_name("streamlit_app.py")
    sys.argv = ["streamlit", "run", str(app_path), *sys.argv[1:]]
    raise SystemExit(streamlit_cli.main())
