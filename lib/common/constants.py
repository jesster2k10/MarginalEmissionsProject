from pathlib import Path
import os

DATA_DIR            = Path(os.path.join(os.path.dirname(__file__), '../../data'))
CLEANED_DATA_DIR    = DATA_DIR / 'cleaned'
PROCESSED_DATA_DIR  = DATA_DIR / 'processed'
RAW_DATA_DIR        = DATA_DIR / 'raw'
BIN_DIR             = Path(os.path.join(os.path.dirname(__file__), '../../bin'))

for directory in [DATA_DIR, CLEANED_DATA_DIR, PROCESSED_DATA_DIR, RAW_DATA_DIR, BIN_DIR]:
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Cannot create {directory} directory")
