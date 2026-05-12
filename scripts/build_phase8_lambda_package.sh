#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/infra/terraform/lakehouse/.terraform/build/news_ai_orchestration_src}"
ZIP_PATH="${ZIP_PATH:-${ROOT_DIR}/infra/terraform/lakehouse/.terraform/build/news_ai_orchestration.zip}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
LAMBDA_PLATFORM="${LAMBDA_PLATFORM:-manylinux2014_x86_64}"

rm -rf "${BUILD_DIR}" "${ZIP_PATH}"
mkdir -p "${BUILD_DIR}" "$(dirname "${ZIP_PATH}")"

cp "${ROOT_DIR}/lambda/news_ai_orchestration.py" "${BUILD_DIR}/news_ai_orchestration.py"
cp -R "${ROOT_DIR}/energy_market" "${BUILD_DIR}/energy_market"
cp -R "${ROOT_DIR}/schemas" "${BUILD_DIR}/schemas"

"${PYTHON_BIN}" -m pip install \
  "jsonschema==4.26.0" \
  "jsonschema-specifications==2025.9.1" \
  "referencing==0.36.2" \
  "typing-extensions==4.15.0" \
  --target "${BUILD_DIR}" \
  --platform "${LAMBDA_PLATFORM}" \
  --implementation cp \
  --python-version "${PYTHON_VERSION}" \
  --only-binary=:all: \
  --upgrade \
  --quiet

"${PYTHON_BIN}" -m pip install \
  "feedparser==6.0.12" \
  "sgmllib3k==1.0.0" \
  --target "${BUILD_DIR}" \
  --upgrade \
  --quiet

BUILD_DIR="${BUILD_DIR}" ZIP_PATH="${ZIP_PATH}" "${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import os
import zipfile
from pathlib import Path

build_dir = Path(os.environ["BUILD_DIR"])
zip_path = Path(os.environ["ZIP_PATH"])

with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(build_dir.rglob("*")):
        if path.is_dir():
            continue
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        arcname = path.relative_to(build_dir).as_posix()
        info = zipfile.ZipInfo(arcname, date_time=(2020, 1, 1, 0, 0, 0))
        info.external_attr = 0o644 << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, path.read_bytes())

print(zip_path)
PY
