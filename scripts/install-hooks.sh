#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOKS_SRC="${ROOT_DIR}/.githooks"
HOOKS_DST="${ROOT_DIR}/.git/hooks"

if [[ ! -d "${ROOT_DIR}/.git" ]]; then
  echo "No .git directory found at ${ROOT_DIR}."
  echo "Initialize/clone the repo first, then re-run this script."
  exit 1
fi

mkdir -p "${HOOKS_DST}"

install -m 0755 "${HOOKS_SRC}/pre-commit" "${HOOKS_DST}/pre-commit"
install -m 0755 "${HOOKS_SRC}/pre-push" "${HOOKS_DST}/pre-push"

echo "Installed git hooks:"
echo "  - ${HOOKS_DST}/pre-commit"
echo "  - ${HOOKS_DST}/pre-push"
