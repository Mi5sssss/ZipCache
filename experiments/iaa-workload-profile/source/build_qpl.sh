#!/usr/bin/env bash
set -euo pipefail
if [[ "$(uname -s)" != Linux || "$(uname -m)" != x86_64 ]]; then
  echo 'This pinned QPL build requires Linux x86-64.' >&2
  exit 1
fi
package_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
qpl_source="$package_dir/work/qpl-src"
qpl_prefix="$package_dir/work/qpl-1.9.0"
if [[ ! -d "$qpl_source" ]]; then
  git clone --depth 1 --branch v1.9.0 https://github.com/intel/qpl.git "$qpl_source"
fi
if [[ "$(git -C "$qpl_source" rev-parse HEAD)" != 1813ccedb90b6468d47cc4ea87ae8b754a9151e1 ]]; then
  echo 'Unexpected QPL revision. Refusing to reset or overwrite it.' >&2
  exit 1
fi
if [[ -n "$(git -C "$qpl_source" status --porcelain)" ]]; then
  echo 'QPL source has local changes. Refusing an unpinned build.' >&2
  exit 1
fi
cmake -S "$qpl_source" -B "$package_dir/work/qpl-build" \
  -DCMAKE_BUILD_TYPE=Release -DQPL_LIBRARY_TYPE=STATIC \
  -DQPL_BUILD_TESTS=OFF -DQPL_BUILD_EXAMPLES=OFF \
  -DCMAKE_INSTALL_PREFIX="$qpl_prefix"
cmake --build "$package_dir/work/qpl-build" -j4
cmake --install "$package_dir/work/qpl-build"
echo "QPL_ROOT=$qpl_prefix"
