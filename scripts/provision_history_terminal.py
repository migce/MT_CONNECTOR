"""One-time explicit installation of the separate history terminal on Windows.

Copies no accounts, saved passwords, charts, Experts or user profiles.
Refuses to overwrite an existing target. Broker secrets are not written here.
"""
from pathlib import Path
import shutil

from src.config import get_settings
from src.mt5.history_connection import validate_history_path


def main():
    s = get_settings()
    validate_history_path(s)
    source = Path(s.mt5_path).parent
    destination = Path(s.history_mt5_path).parent
    if destination.exists():
        raise RuntimeError("History installation already exists; inspect, do not overwrite")
    if not (source / "terminal64.exe").is_file():
        raise FileNotFoundError("Source terminal not found")
    destination.mkdir(parents=True)
    (destination / "Config").mkdir()
    for name in ("terminal64.exe", "Config/servers.dat", "Config/terminal.lic"):
        src = source / name
        if src.is_file():
            shutil.copy2(src, destination / name)
    config = "[Experts]\nEnabled=0\nAllowLiveTrading=0\nAllowDllImport=0\n[Charts]\nMaxCharts=0\n[StartUp]\nAutoUpdate=0\nNewsEnable=0\n"
    (destination / "history.ini").write_text(config, encoding="utf-16")
    print("Dedicated history terminal installed; no login performed")


if __name__ == "__main__":
    main()
