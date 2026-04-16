#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRAM_TIER="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BIN_DIR="${BIN_DIR:-${DRAM_TIER}/build_check/bin}"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"
THREADS_LIST="${THREADS_LIST:-1 2 4 8 16 32}"
SHARDS_LIST="${SHARDS_LIST:-1}"
LANDING_LIST="${LANDING_LIST:-512}"
BTREE_USE_SILESIA="${BTREE_USE_SILESIA:-1}"
BTREE_VALUE_BYTES="${BTREE_VALUE_BYTES:-128}"

BENCH_DURATION_SEC="${BENCH_DURATION_SEC:-5}"
BENCH_WARMUP_KEYS="${BENCH_WARMUP_KEYS:-10000}"
BENCH_KEY_SPACE="${BENCH_KEY_SPACE:-50000}"
BTREE_DURATION_SEC="${BTREE_DURATION_SEC:-2}"
BTREE_KEY_SPACE="${BTREE_KEY_SPACE:-50000}"
BTREE_HOT_PCT="${BTREE_HOT_PCT:-100}"

THROUGHPUT_BIN="${BIN_DIR}/bpt_compressed_throughput_bench"
MIXED_BIN="${BIN_DIR}/bpt_compressed_mixed_concurrency"

if [[ ! -x "${THROUGHPUT_BIN}" ]]; then
    echo "missing executable: ${THROUGHPUT_BIN}" >&2
    exit 1
fi
if [[ ! -x "${MIXED_BIN}" ]]; then
    echo "missing executable: ${MIXED_BIN}" >&2
    exit 1
fi

mkdir -p "${RESULTS_DIR}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${RESULTS_DIR}/${RUN_ID}"
mkdir -p "${RUN_DIR}"
SUMMARY="${RUN_DIR}/summary.tsv"

printf "case\tvalue_source\tvalue_bytes\tlanding_bytes\tshards\tthreads\tcodec\tqps\tmismatches\tratio\tsaved_pct\tcompressed_bytes\ttotal_bytes\tlog\n" > "${SUMMARY}"

append_mixed_summary() {
    local case_name="$1"
    local landing="$2"
    local shards="$3"
    local threads="$4"
    local log_file="$5"

    awk -v case_name="${case_name}" -v value_source="${BTREE_USE_SILESIA}" -v value_bytes="${BTREE_VALUE_BYTES}" -v landing="${landing}" -v shards="${shards}" -v threads="${threads}" -v log_file="${log_file}" '
        /mixed_concurrency\[/ {
            codec = $0
            sub(/^.*\[/, "", codec)
            sub(/\].*$/, "", codec)
            qps = $0
            sub(/^.*qps=/, "", qps)
            sub(/ .*/, "", qps)
            mismatches = $0
            sub(/^.*mismatches=/, "", mismatches)
            sub(/ .*/, "", mismatches)
            stats = $0
            if (stats ~ /stats=[0-9]+\/[0-9]+/) {
                sub(/^.*stats=/, "", stats)
                sub(/ .*/, "", stats)
                split(stats, parts, "/")
                compressed = parts[1]
                total = parts[2]
            } else {
                compressed = "NA"
                total = "NA"
            }
            ratio = $0
            if (ratio ~ /ratio=[0-9.]+/) {
                sub(/^.*ratio=/, "", ratio)
                sub(/ .*/, "", ratio)
            } else {
                ratio = "NA"
            }
            saved = $0
            if (saved ~ /saved_pct=[-0-9.]+/) {
                sub(/^.*saved_pct=/, "", saved)
                sub(/ .*/, "", saved)
            } else {
                saved = "NA"
            }
            print case_name "\t" value_source "\t" value_bytes "\t" landing "\t" shards "\t" threads "\t" codec "\t" qps "\t" mismatches "\t" ratio "\t" saved "\t" compressed "\t" total "\t" log_file
        }
    ' "${log_file}" >> "${SUMMARY}"
}

append_bench_summary() {
    local case_name="$1"
    local landing="$2"
    local shards="$3"
    local threads="$4"
    local log_file="$5"

    awk -v case_name="${case_name}" -v value_source="${BTREE_USE_SILESIA}" -v value_bytes="${BTREE_VALUE_BYTES}" -v landing="${landing}" -v shards="${shards}" -v threads="${threads}" -v log_file="${log_file}" '
        $2 == "mixed" {
            line = $0
            mismatches = line
            if (mismatches ~ /mismatches=[0-9]+/) {
                sub(/^.*mismatches=/, "", mismatches)
                sub(/ .*/, "", mismatches)
            } else {
                mismatches = "NA"
            }
            mem = line
            if (mem ~ /mem=[0-9]+\/[0-9]+/) {
                sub(/^.*mem=/, "", mem)
                sub(/ .*/, "", mem)
                split(mem, parts, "/")
                compressed = parts[1]
                total = parts[2]
            } else {
                compressed = "NA"
                total = "NA"
            }
            ratio = line
            if (ratio ~ /ratio=[0-9.]+/) {
                sub(/^.*ratio=/, "", ratio)
                sub(/ .*/, "", ratio)
            } else {
                ratio = "NA"
            }
            saved = line
            if (saved ~ /saved_pct=[-0-9.]+/) {
                sub(/^.*saved_pct=/, "", saved)
                sub(/ .*/, "", saved)
            } else {
                saved = "NA"
            }
            print case_name "\t" value_source "\t" value_bytes "\t" landing "\t" shards "\t" threads "\t" $1 "\t" $5 "\t" mismatches "\t" ratio "\t" saved "\t" compressed "\t" total "\t" log_file
        }
    ' "${log_file}" >> "${SUMMARY}"
}

run_bench_mixed() {
    local landing="$1"
    local shards="$2"
    local threads="$3"
    local log_file="${RUN_DIR}/bench_mixed_lb${landing}_s${shards}_t${threads}.log"

    echo "bench_mixed landing=${landing} shards=${shards} threads=${threads}"
    if ! BTREE_USE_SILESIA="${BTREE_USE_SILESIA}" \
         BTREE_VALUE_BYTES="${BTREE_VALUE_BYTES}" \
         BTREE_LANDING_BUFFER_BYTES="${landing}" \
         BTREE_SHARDS="${shards}" \
         BENCH_DURATION_SEC="${BENCH_DURATION_SEC}" \
         BENCH_WARMUP_KEYS="${BENCH_WARMUP_KEYS}" \
         BENCH_KEY_SPACE="${BENCH_KEY_SPACE}" \
         BENCH_THREADS="${threads}" \
         "${THROUGHPUT_BIN}" 2>&1 | tee "${log_file}"; then
        echo "bench_mixed failed shards=${shards} threads=${threads}; see ${log_file}" >&2
        exit 1
    fi
    append_bench_summary "bench_mixed" "${landing}" "${shards}" "${threads}" "${log_file}"
}

run_mixed_case() {
    local case_name="$1"
    local landing="$2"
    local shards="$3"
    local threads="$4"
    local read_pct="$5"
    local write_pct="$6"
    local delete_pct="$7"
    local scan_pct="$8"
    local log_file="${RUN_DIR}/${case_name}_lb${landing}_s${shards}_t${threads}.log"

    echo "${case_name} landing=${landing} shards=${shards} threads=${threads} mix=${read_pct}/${write_pct}/${delete_pct}/${scan_pct}"
    if ! BTREE_USE_SILESIA="${BTREE_USE_SILESIA}" \
         BTREE_VALUE_BYTES="${BTREE_VALUE_BYTES}" \
         BTREE_LANDING_BUFFER_BYTES="${landing}" \
         BTREE_SHARDS="${shards}" \
         BTREE_DURATION_SEC="${BTREE_DURATION_SEC}" \
         BTREE_KEY_SPACE="${BTREE_KEY_SPACE}" \
         BTREE_HOT_PCT="${BTREE_HOT_PCT}" \
         BTREE_READ_PCT="${read_pct}" \
         BTREE_WRITE_PCT="${write_pct}" \
         BTREE_DELETE_PCT="${delete_pct}" \
         BTREE_SCAN_PCT="${scan_pct}" \
         BTREE_THREADS="${threads}" \
         "${MIXED_BIN}" 2>&1 | tee "${log_file}"; then
        echo "${case_name} failed shards=${shards} threads=${threads}; see ${log_file}" >&2
        exit 1
    fi
    append_mixed_summary "${case_name}" "${landing}" "${shards}" "${threads}" "${log_file}"
}

for landing in ${LANDING_LIST}; do
    for shards in ${SHARDS_LIST}; do
        for threads in ${THREADS_LIST}; do
            run_mixed_case "read_only" "${landing}" "${shards}" "${threads}" 100 0 0 0
            run_mixed_case "read_heavy" "${landing}" "${shards}" "${threads}" 80 15 5 0
            run_mixed_case "point_mixed" "${landing}" "${shards}" "${threads}" 55 35 10 0
            run_mixed_case "mixed_with_scan" "${landing}" "${shards}" "${threads}" 50 35 10 5
            run_mixed_case "write_heavy" "${landing}" "${shards}" "${threads}" 20 70 10 0
            run_bench_mixed "${landing}" "${shards}" "${threads}"
        done
    done
done

echo "summary: ${SUMMARY}"
column -t -s $'\t' "${SUMMARY}" || cat "${SUMMARY}"
