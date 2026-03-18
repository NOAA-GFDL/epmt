#!/bin/bash
# compile_papiex.sh — build papiex during pip install (best-effort)
#
# This script is invoked by setup.py during 'pip install' to compile the
# papiex C library and install its shared libraries into epmt/lib/ so they
# are picked up by the [tool.setuptools.package-data] glob "lib/*.so*".
#
# The script always exits 0 (graceful skip) when build prerequisites are
# missing or when the source cannot be obtained.  It exits non-zero only
# when a compilation is attempted and fails; setup.py catches that case
# and prints a warning while allowing the install to proceed.
#
# Environment variables:
#   PAPIEX_SRC_BRANCH   - Branch/tag to download (default: main)
#   CONFIG_PAPIEX_PAPI  - Enable PAPI hardware counters (default: n)
#   CONFIG_PAPIEX_DEBUG - Enable debug build (default: n)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Vendored source (populated by 'make vendor-papiex')
PAPIEX_VENDOR_SRC="${SCRIPT_DIR}/vendor/papiex"

# Installation prefix: papiex installs libs into ${INSTALL_PREFIX}/lib/,
# which maps to src/epmt/lib/ and is picked up as package-data.
INSTALL_PREFIX="${SCRIPT_DIR}/epmt"

PAPIEX_SRC_BRANCH="${PAPIEX_SRC_BRANCH:-main}"
PAPIEX_SRC_URL="https://github.com/NOAA-GFDL/papiex/archive/${PAPIEX_SRC_BRANCH}.tar.gz"
CONFIG_PAPIEX_PAPI="${CONFIG_PAPIEX_PAPI:-n}"
CONFIG_PAPIEX_DEBUG="${CONFIG_PAPIEX_DEBUG:-n}"

echo "compile_papiex.sh: starting papiex build (best-effort)"
echo "  SCRIPT_DIR=${SCRIPT_DIR}"
echo "  INSTALL_PREFIX=${INSTALL_PREFIX}"

# --- skip if shared libraries are already present (e.g. pre-bundled in sdist) ---
if ls "${INSTALL_PREFIX}/lib/"*.so* 2>/dev/null | head -1 | grep -q .; then
    echo "compile_papiex.sh: papiex shared libraries already present in" \
         "${INSTALL_PREFIX}/lib/ — skipping compilation."
    exit 0
fi

# --- check for required build tools ---
_missing=""
command -v gcc  &>/dev/null || _missing="${_missing} gcc"
command -v make &>/dev/null || _missing="${_missing} make"
if [ -n "${_missing}" ]; then
    echo "WARNING: the following build tools are not available:${_missing}"
    echo "         Skipping papiex compilation."
    echo "         EPMT will still install but hardware counter collection"
    echo "         will be unavailable.  Run 'epmt check' to verify status."
    exit 0
fi

# --- determine papiex source location ---
PAPIEX_SRC_DIR=""
PAPIEX_TMPDIR=""

if [ -d "${PAPIEX_VENDOR_SRC}" ] && [ -f "${PAPIEX_VENDOR_SRC}/Makefile" ]; then
    echo "compile_papiex.sh: using vendored papiex source at ${PAPIEX_VENDOR_SRC}"
    PAPIEX_SRC_DIR="${PAPIEX_VENDOR_SRC}"
else
    if ! command -v curl &>/dev/null; then
        echo "WARNING: curl not found and no vendored papiex source available."
        echo "         Skipping papiex compilation."
        exit 0
    fi

    echo "compile_papiex.sh: downloading papiex source from ${PAPIEX_SRC_URL}"
    PAPIEX_TMPDIR="$(mktemp -d)"
    # SC2064: intentional — capture PAPIEX_TMPDIR value now so the trap
    # removes the correct directory even if the variable were later changed.
    # shellcheck disable=SC2064
    trap 'rm -rf "${PAPIEX_TMPDIR}"' EXIT

    if ! curl -L --fail --retry 3 --retry-delay 5 \
            -o "${PAPIEX_TMPDIR}/papiex.tar.gz" \
            "${PAPIEX_SRC_URL}"; then
        echo "WARNING: Failed to download papiex source from ${PAPIEX_SRC_URL}."
        echo "         Skipping papiex compilation."
        exit 0
    fi

    # Determine the top-level directory name from the listing before
    # extraction to avoid a second decompression pass.
    TOP_DIR=$(tar -ztf "${PAPIEX_TMPDIR}/papiex.tar.gz" | head -1 | cut -d/ -f1)
    tar -zxf "${PAPIEX_TMPDIR}/papiex.tar.gz" -C "${PAPIEX_TMPDIR}"
    PAPIEX_SRC_DIR="${PAPIEX_TMPDIR}/${TOP_DIR}"
fi

# --- compile and install ---
mkdir -p "${INSTALL_PREFIX}/lib"

echo "compile_papiex.sh: compiling papiex"
echo "  PAPIEX_SRC_DIR=${PAPIEX_SRC_DIR}"
echo "  CONFIG_PAPIEX_PAPI=${CONFIG_PAPIEX_PAPI}"
echo "  CONFIG_PAPIEX_DEBUG=${CONFIG_PAPIEX_DEBUG}"

make -C "${PAPIEX_SRC_DIR}" \
    PREFIX="${INSTALL_PREFIX}" \
    CONFIG_PAPIEX_PAPI="${CONFIG_PAPIEX_PAPI}" \
    CONFIG_PAPIEX_DEBUG="${CONFIG_PAPIEX_DEBUG}" \
    install

echo "compile_papiex.sh: papiex compiled and installed to ${INSTALL_PREFIX}"
exit 0
