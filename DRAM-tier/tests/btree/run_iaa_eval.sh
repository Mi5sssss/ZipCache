#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRAM_TIER="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BIN_DIR="${BIN_DIR:-${DRAM_TIER}/build_check/bin}"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results/iaa_eval}"

MIXED_BIN="${BIN_DIR}/bpt_compressed_mixed_concurrency"
TAIL_BIN="${BIN_DIR}/tail_latency_compare"

THREADS_LIST="${THREADS_LIST:-4 8 16 32}"
BTREE_DURATION_SEC="${BTREE_DURATION_SEC:-5}"
BTREE_KEY_SPACE="${BTREE_KEY_SPACE:-50000}"
BTREE_HOT_PCT="${BTREE_HOT_PCT:-100}"
BTREE_SHARDS="${BTREE_SHARDS:-8}"
BTREE_LANDING_BUFFER_BYTES="${BTREE_LANDING_BUFFER_BYTES:-512}"
BTREE_USE_SILESIA="${BTREE_USE_SILESIA:-1}"
BTREE_VALUE_BYTES="${BTREE_VALUE_BYTES:-128}"
BTREE_MEASURE_LATENCY="${BTREE_MEASURE_LATENCY:-1}"
BTREE_LATENCY_SAMPLES_PER_THREAD="${BTREE_LATENCY_SAMPLES_PER_THREAD:-4096}"
BTREE_QPL_JOB_CACHE="${BTREE_QPL_JOB_CACHE:-thread}"
BTREE_ZLIB_STREAM_CACHE="${BTREE_ZLIB_STREAM_CACHE:-none}"
LANDING_LIST="${LANDING_LIST:-${BTREE_LANDING_BUFFER_BYTES}}"
RUN_BG_SWEEP="${RUN_BG_SWEEP:-0}"
BG_SWEEP_VALUES="${BG_SWEEP_VALUES:-0 1}"
BTREE_BG_COMPACTION="${BTREE_BG_COMPACTION:-0}"
BTREE_BG_THREADS="${BTREE_BG_THREADS:-1}"
BTREE_BG_LANDING_HIGH_WATERMARK_PCT="${BTREE_BG_LANDING_HIGH_WATERMARK_PCT:-75}"
BTREE_BG_SCAN_INTERVAL_US="${BTREE_BG_SCAN_INTERVAL_US:-1000}"
BTREE_BG_QUEUE_CAPACITY="${BTREE_BG_QUEUE_CAPACITY:-4096}"
BTREE_BG_CODEC="${BTREE_BG_CODEC:-all}"
COLLECT_CPU="${COLLECT_CPU:-1}"
TIME_BIN="${TIME_BIN:-/usr/bin/time}"

RUN_IAA="${RUN_IAA:-0}"
RUN_QPL_DYNAMIC="${RUN_QPL_DYNAMIC:-1}"
RUN_ZLIB_ACCEL="${RUN_ZLIB_ACCEL:-0}"
ZLIB_ACCEL_SO="${ZLIB_ACCEL_SO:-}"
SOFTWARE_CODECS="${SOFTWARE_CODECS:-lz4 qpl}"
IAA_CODECS="${IAA_CODECS:-qpl}"
ZLIB_ACCEL_CODECS="${ZLIB_ACCEL_CODECS:-zlib_accel}"

RUN_TAIL_LATENCY="${RUN_TAIL_LATENCY:-0}"
TAIL_LATENCY_KEY_COUNT="${TAIL_LATENCY_KEY_COUNT:-32768}"
TAIL_LATENCY_SAMPLE_COUNT="${TAIL_LATENCY_SAMPLE_COUNT:-8192}"
TAIL_LATENCY_WARMUP="${TAIL_LATENCY_WARMUP:-8192}"

if [[ ! -x "${MIXED_BIN}" ]]; then
    echo "missing executable: ${MIXED_BIN}" >&2
    echo "build first: cmake -S DRAM-tier -B DRAM-tier/build_check && cmake --build DRAM-tier/build_check -j\$(nproc)" >&2
    exit 1
fi

if [[ "${RUN_TAIL_LATENCY}" == "1" && ! -x "${TAIL_BIN}" ]]; then
    echo "missing executable: ${TAIL_BIN}" >&2
    exit 1
fi

if [[ "${RUN_ZLIB_ACCEL}" == "1" && -z "${ZLIB_ACCEL_SO}" ]]; then
    echo "RUN_ZLIB_ACCEL=1 requires ZLIB_ACCEL_SO=<zlib-accel-build>/libzlib_accel.so" >&2
    exit 1
fi

if [[ -n "${ZLIB_ACCEL_SO}" && ! -r "${ZLIB_ACCEL_SO}" ]]; then
    echo "ZLIB_ACCEL_SO is not readable: ${ZLIB_ACCEL_SO}" >&2
    exit 1
fi

mkdir -p "${RESULTS_DIR}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${RESULTS_DIR}/${RUN_ID}"
mkdir -p "${RUN_DIR}"
SUMMARY="${RUN_DIR}/summary.tsv"
TAIL_SUMMARY="${RUN_DIR}/tail_latency_summary.tsv"

printf "run\tworkload\tthreads\tlanding_bytes\tbg_compaction\tbg_threads\tbg_watermark\tbg_codec\tcodec\tengine\tqpl_mode\tqpl_job_cache\tzlib_stream_cache\tqps\tmismatches\tratio\tsaved_pct\tread_p50_us\tread_p99_us\tread_p999_us\twrite_p50_us\twrite_p99_us\twrite_p999_us\tcompressed_bytes\ttotal_bytes\twall_sec\tuser_sec\tsys_sec\tcpu_pct\tcpu_cores\tqps_per_cpu_core\tmax_rss_kb\tvoluntary_cs\tinvoluntary_cs\tbg_passes\tbg_compactions\tbg_trylock_misses\tbg_skipped\tbg_errors\tfg_landing_full\tfg_sync_compactions\tfg_sync_compaction_errors\tfg_split_fallbacks\tbg_enqueue_attempts\tbg_enqueued\tbg_enqueue_duplicates\tbg_queue_full\tbg_queue_pops\tqpl_compress_calls\tqpl_decompress_calls\tqpl_tls_jobs\tqpl_pool_jobs\tqpl_errors\tzlib_compress_calls\tzlib_decompress_calls\tzlib_stream_reuses\tzlib_stream_inits\tzlib_errors\tlog\tcpu_log\n" > "${SUMMARY}"
printf "run\tworkload\tcodec\tengine\tqpl_mode\tget_p50_us\tget_p90_us\tget_p99_us\tget_p999_us\tget_max_us\tget_avg_us\tput_p50_us\tput_p90_us\tput_p99_us\tput_p999_us\tput_max_us\tput_avg_us\tthroughput_mops\tlog\tcsv\n" > "${TAIL_SUMMARY}"

cpu_metric() {
    local metric_file="$1"
    local name="$2"
    local default_value="${3:-NA}"

    if [[ ! -r "${metric_file}" ]]; then
        printf "%s" "${default_value}"
        return
    fi

    awk -F= -v name="${name}" -v default_value="${default_value}" '
        $1 == name {
            print $2
            found = 1
            exit
        }
        END {
            if (!found) {
                print default_value
            }
        }
    ' "${metric_file}"
}

cpu_cores_from_pct() {
    local cpu_pct="$1"
    local cpu_pct_number="${cpu_pct%\%}"

    awk -v pct="${cpu_pct_number}" '
        BEGIN {
            if (pct ~ /^[0-9.]+$/) {
                printf "%.2f", pct / 100.0
            } else {
                print "NA"
            }
        }
    '
}

run_with_optional_cpu_metrics() {
    local log_file="$1"
    local cpu_file="$2"
    shift 2

    if [[ "${COLLECT_CPU}" == "1" && -x "${TIME_BIN}" ]]; then
        if ! "${TIME_BIN}" \
            -f "wall_sec=%e\nuser_sec=%U\nsys_sec=%S\ncpu_pct=%P\nmax_rss_kb=%M\nvoluntary_cs=%w\ninvoluntary_cs=%c" \
            -o "${cpu_file}" \
            "$@" > "${log_file}" 2>&1; then
            cat "${log_file}"
            return 1
        fi
        cat "${log_file}"
        return 0
    fi

    : > "${cpu_file}"
    "$@" 2>&1 | tee "${log_file}"
}

append_mixed_summary() {
    local run_label="$1"
    local workload="$2"
    local threads="$3"
    local qpl_path="$4"
    local qpl_mode="$5"
    local zlib_backend="$6"
    local log_file="$7"
    local landing_bytes="$8"
    local bg_compaction="$9"
    local bg_threads="${10}"
    local bg_watermark="${11}"
    local bg_codec="${12}"
    local cpu_file="${13}"

    local wall_sec user_sec sys_sec cpu_pct cpu_cores max_rss_kb voluntary_cs involuntary_cs
    wall_sec="$(cpu_metric "${cpu_file}" "wall_sec")"
    user_sec="$(cpu_metric "${cpu_file}" "user_sec")"
    sys_sec="$(cpu_metric "${cpu_file}" "sys_sec")"
    cpu_pct="$(cpu_metric "${cpu_file}" "cpu_pct")"
    cpu_cores="$(cpu_cores_from_pct "${cpu_pct}")"
    max_rss_kb="$(cpu_metric "${cpu_file}" "max_rss_kb")"
    voluntary_cs="$(cpu_metric "${cpu_file}" "voluntary_cs")"
    involuntary_cs="$(cpu_metric "${cpu_file}" "involuntary_cs")"

    awk -v run_label="${run_label}" \
        -v workload="${workload}" \
        -v threads="${threads}" \
        -v landing_bytes="${landing_bytes}" \
        -v bg_compaction="${bg_compaction}" \
        -v bg_threads="${bg_threads}" \
        -v bg_watermark="${bg_watermark}" \
        -v bg_codec="${bg_codec}" \
        -v qpl_path="${qpl_path}" \
        -v qpl_mode="${qpl_mode}" \
        -v qpl_job_cache="${BTREE_QPL_JOB_CACHE}" \
        -v zlib_stream_cache="${BTREE_ZLIB_STREAM_CACHE}" \
        -v zlib_backend="${zlib_backend}" \
        -v log_file="${log_file}" \
        -v cpu_file="${cpu_file}" \
        -v wall_sec="${wall_sec}" \
        -v user_sec="${user_sec}" \
        -v sys_sec="${sys_sec}" \
        -v cpu_pct="${cpu_pct}" \
        -v cpu_cores="${cpu_cores}" \
        -v max_rss_kb="${max_rss_kb}" \
        -v voluntary_cs="${voluntary_cs}" \
        -v involuntary_cs="${involuntary_cs}" '
        function field(line, name, default_value, value) {
            value = line
            if (value ~ (name "=[^ ]+")) {
                sub("^.*" name "=", "", value)
                sub(/ .*/, "", value)
                return value
            }
            return default_value
        }
        /mixed_concurrency\[/ {
            codec = $0
            sub(/^.*\[/, "", codec)
            sub(/\].*$/, "", codec)

            engine = "cpu"
            mode = "NA"
            if (codec == "qpl") {
                engine = qpl_path
                mode = qpl_mode
            } else if (codec == "zlib_accel") {
                engine = zlib_backend
            }

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

            read_p50 = field($0, "read_p50_us", "NA")
            read_p99 = field($0, "read_p99_us", "NA")
            read_p999 = field($0, "read_p999_us", "NA")
            write_p50 = field($0, "write_p50_us", "NA")
            write_p99 = field($0, "write_p99_us", "NA")
            write_p999 = field($0, "write_p999_us", "NA")
            row_wall_sec = field($0, "bench_wall_sec", wall_sec)
            row_user_sec = field($0, "bench_user_sec", user_sec)
            row_sys_sec = field($0, "bench_sys_sec", sys_sec)
            row_cpu_pct = field($0, "bench_cpu_pct", cpu_pct)
            row_cpu_cores = field($0, "bench_cpu_cores", cpu_cores)

            bg_passes = $0
            if (bg_passes ~ /bg_passes=[0-9]+/) {
                sub(/^.*bg_passes=/, "", bg_passes)
                sub(/ .*/, "", bg_passes)
            } else {
                bg_passes = "NA"
            }

            bg_compactions = $0
            if (bg_compactions ~ /bg_compactions=[0-9]+/) {
                sub(/^.*bg_compactions=/, "", bg_compactions)
                sub(/ .*/, "", bg_compactions)
            } else {
                bg_compactions = "NA"
            }

            bg_trylock_misses = $0
            if (bg_trylock_misses ~ /bg_trylock_misses=[0-9]+/) {
                sub(/^.*bg_trylock_misses=/, "", bg_trylock_misses)
                sub(/ .*/, "", bg_trylock_misses)
            } else {
                bg_trylock_misses = "NA"
            }

            bg_skipped = $0
            if (bg_skipped ~ /bg_skipped=[0-9]+/) {
                sub(/^.*bg_skipped=/, "", bg_skipped)
                sub(/ .*/, "", bg_skipped)
            } else {
                bg_skipped = "NA"
            }

            bg_errors = $0
            if (bg_errors ~ /bg_errors=[0-9]+/) {
                sub(/^.*bg_errors=/, "", bg_errors)
                sub(/ .*/, "", bg_errors)
            } else {
                bg_errors = "NA"
            }

            fg_landing_full = $0
            if (fg_landing_full ~ /fg_landing_full=[0-9]+/) {
                sub(/^.*fg_landing_full=/, "", fg_landing_full)
                sub(/ .*/, "", fg_landing_full)
            } else {
                fg_landing_full = "NA"
            }

            fg_sync_compactions = $0
            if (fg_sync_compactions ~ /fg_sync_compactions=[0-9]+/) {
                sub(/^.*fg_sync_compactions=/, "", fg_sync_compactions)
                sub(/ .*/, "", fg_sync_compactions)
            } else {
                fg_sync_compactions = "NA"
            }

            fg_sync_compaction_errors = $0
            if (fg_sync_compaction_errors ~ /fg_sync_compaction_errors=[0-9]+/) {
                sub(/^.*fg_sync_compaction_errors=/, "", fg_sync_compaction_errors)
                sub(/ .*/, "", fg_sync_compaction_errors)
            } else {
                fg_sync_compaction_errors = "NA"
            }

            fg_split_fallbacks = $0
            if (fg_split_fallbacks ~ /fg_split_fallbacks=[0-9]+/) {
                sub(/^.*fg_split_fallbacks=/, "", fg_split_fallbacks)
                sub(/ .*/, "", fg_split_fallbacks)
            } else {
                fg_split_fallbacks = "NA"
            }

            bg_enqueue_attempts = $0
            if (bg_enqueue_attempts ~ /bg_enqueue_attempts=[0-9]+/) {
                sub(/^.*bg_enqueue_attempts=/, "", bg_enqueue_attempts)
                sub(/ .*/, "", bg_enqueue_attempts)
            } else {
                bg_enqueue_attempts = "NA"
            }

            bg_enqueued = $0
            if (bg_enqueued ~ /bg_enqueued=[0-9]+/) {
                sub(/^.*bg_enqueued=/, "", bg_enqueued)
                sub(/ .*/, "", bg_enqueued)
            } else {
                bg_enqueued = "NA"
            }

            bg_enqueue_duplicates = $0
            if (bg_enqueue_duplicates ~ /bg_enqueue_duplicates=[0-9]+/) {
                sub(/^.*bg_enqueue_duplicates=/, "", bg_enqueue_duplicates)
                sub(/ .*/, "", bg_enqueue_duplicates)
            } else {
                bg_enqueue_duplicates = "NA"
            }

            bg_queue_full = $0
            if (bg_queue_full ~ /bg_queue_full=[0-9]+/) {
                sub(/^.*bg_queue_full=/, "", bg_queue_full)
                sub(/ .*/, "", bg_queue_full)
            } else {
                bg_queue_full = "NA"
            }

            bg_queue_pops = $0
            if (bg_queue_pops ~ /bg_queue_pops=[0-9]+/) {
                sub(/^.*bg_queue_pops=/, "", bg_queue_pops)
                sub(/ .*/, "", bg_queue_pops)
            } else {
                bg_queue_pops = "NA"
            }

            qpl_compress_calls = field($0, "qpl_compress_calls", "NA")
            qpl_decompress_calls = field($0, "qpl_decompress_calls", "NA")
            qpl_tls_jobs = field($0, "qpl_tls_jobs", "NA")
            qpl_pool_jobs = field($0, "qpl_pool_jobs", "NA")
            qpl_errors = field($0, "qpl_errors", "NA")
            zlib_compress_calls = field($0, "zlib_compress_calls", "NA")
            zlib_decompress_calls = field($0, "zlib_decompress_calls", "NA")
            zlib_stream_reuses = field($0, "zlib_stream_reuses", "NA")
            zlib_stream_inits = field($0, "zlib_stream_inits", "NA")
            zlib_errors = field($0, "zlib_errors", "NA")

            if (qps ~ /^[0-9.]+$/ && row_cpu_cores ~ /^[0-9.]+$/ && row_cpu_cores > 0) {
                qps_per_cpu_core = sprintf("%.1f", qps / row_cpu_cores)
            } else {
                qps_per_cpu_core = "NA"
            }

            print run_label "\t" workload "\t" threads "\t" landing_bytes "\t" bg_compaction "\t" bg_threads "\t" bg_watermark "\t" bg_codec "\t" codec "\t" engine "\t" mode "\t" qpl_job_cache "\t" zlib_stream_cache "\t" qps "\t" mismatches "\t" ratio "\t" saved "\t" read_p50 "\t" read_p99 "\t" read_p999 "\t" write_p50 "\t" write_p99 "\t" write_p999 "\t" compressed "\t" total "\t" row_wall_sec "\t" row_user_sec "\t" row_sys_sec "\t" row_cpu_pct "\t" row_cpu_cores "\t" qps_per_cpu_core "\t" max_rss_kb "\t" voluntary_cs "\t" involuntary_cs "\t" bg_passes "\t" bg_compactions "\t" bg_trylock_misses "\t" bg_skipped "\t" bg_errors "\t" fg_landing_full "\t" fg_sync_compactions "\t" fg_sync_compaction_errors "\t" fg_split_fallbacks "\t" bg_enqueue_attempts "\t" bg_enqueued "\t" bg_enqueue_duplicates "\t" bg_queue_full "\t" bg_queue_pops "\t" qpl_compress_calls "\t" qpl_decompress_calls "\t" qpl_tls_jobs "\t" qpl_pool_jobs "\t" qpl_errors "\t" zlib_compress_calls "\t" zlib_decompress_calls "\t" zlib_stream_reuses "\t" zlib_stream_inits "\t" zlib_errors "\t" log_file "\t" cpu_file
        }
    ' "${log_file}" >> "${SUMMARY}"
}

append_tail_latency_summary() {
    local run_label="$1"
    local qpl_path="$2"
    local qpl_mode="$3"
    local zlib_backend="$4"
    local log_file="$5"
    local csv_file="$6"

    awk -v run_label="${run_label}" \
        -v qpl_path="${qpl_path}" \
        -v qpl_mode="${qpl_mode}" \
        -v zlib_backend="${zlib_backend}" \
        -v log_file="${log_file}" \
        -v csv_file="${csv_file}" '
        function reset_metrics() {
            get_p50 = get_p90 = get_p99 = get_p999 = get_max = get_avg = "NA"
            put_p50 = put_p90 = put_p99 = put_p999 = put_max = put_avg = "NA"
            throughput = "NA"
        }
        function emit_if_ready() {
            if (label == "" || throughput == "NA") {
                return
            }
            codec = "unknown"
            engine = "cpu"
            mode = "NA"
            if (label ~ /^LZ4/) {
                codec = "lz4"
                engine = "cpu"
            } else if (label ~ /^QPL/) {
                codec = "qpl"
                engine = qpl_path
                mode = qpl_mode
            } else if (label ~ /^zlib_accel/) {
                codec = "zlib_accel"
                engine = zlib_backend
            }

            workload = "unknown"
            if (label ~ /ReadOnly/) {
                workload = "read_only"
            } else if (label ~ /Mixed/) {
                workload = "mixed_80r20w"
            }

            print run_label "\t" workload "\t" codec "\t" engine "\t" mode "\t" \
                  get_p50 "\t" get_p90 "\t" get_p99 "\t" get_p999 "\t" get_max "\t" get_avg "\t" \
                  put_p50 "\t" put_p90 "\t" put_p99 "\t" put_p999 "\t" put_max "\t" put_avg "\t" \
                  throughput "\t" log_file "\t" csv_file
        }
        BEGIN {
            label = ""
            reset_metrics()
        }
        /^[^[:space:]].*(ReadOnly|Mixed)/ {
            emit_if_ready()
            label = $0
            reset_metrics()
            next
        }
        /^[[:space:]]+get[[:space:]]*:/ {
            if ($0 ~ /n\/a/) {
                next
            }
            get_p50 = $4
            get_p90 = $6
            get_p99 = $8
            if ($9 == "p999") {
                get_p999 = $10
                get_max = $12
                get_avg = $14
            } else {
                get_p999 = "NA"
                get_max = $10
                get_avg = $12
            }
            next
        }
        /^[[:space:]]+put[[:space:]]*:/ {
            if ($0 ~ /n\/a/) {
                next
            }
            put_p50 = $4
            put_p90 = $6
            put_p99 = $8
            if ($9 == "p999") {
                put_p999 = $10
                put_max = $12
                put_avg = $14
            } else {
                put_p999 = "NA"
                put_max = $10
                put_avg = $12
            }
            next
        }
        /^[[:space:]]+throughput:/ {
            throughput = $2
            emit_if_ready()
            label = ""
            reset_metrics()
            next
        }
        END {
            emit_if_ready()
        }
    ' "${log_file}" >> "${TAIL_SUMMARY}"
}

run_mixed_case() {
    local run_label="$1"
    local qpl_path="$2"
    local qpl_mode="$3"
    local zlib_backend="$4"
    local preload="$5"
    local workload="$6"
    local read_pct="$7"
    local write_pct="$8"
    local delete_pct="$9"
    local scan_pct="${10}"
    local threads="${11}"
    local landing_bytes="${12}"
    local bg_compaction="${13}"
    local codec_filter="${14}"
    local bg_label="bg${bg_compaction}_${BTREE_BG_CODEC}_w${BTREE_BG_LANDING_HIGH_WATERMARK_PCT}"

    local log_file="${RUN_DIR}/${run_label}_${codec_filter}_${workload}_t${threads}_lb${landing_bytes}_${bg_label}.log"
    local cpu_file="${RUN_DIR}/${run_label}_${codec_filter}_${workload}_t${threads}_lb${landing_bytes}_${bg_label}.cpu"
    echo "run=${run_label} codec=${codec_filter} workload=${workload} threads=${threads} landing=${landing_bytes} bg=${bg_compaction}/${BTREE_BG_CODEC} qpl=${qpl_path}/${qpl_mode} zlib=${zlib_backend}"

    local env_args=(
        "BTREE_CODEC_FILTER=${codec_filter}"
        "BTREE_QPL_PATH=${qpl_path}"
        "BTREE_QPL_MODE=${qpl_mode}"
        "BTREE_QPL_JOB_CACHE=${BTREE_QPL_JOB_CACHE}"
        "BTREE_USE_SILESIA=${BTREE_USE_SILESIA}"
        "BTREE_VALUE_BYTES=${BTREE_VALUE_BYTES}"
        "BTREE_LANDING_BUFFER_BYTES=${landing_bytes}"
        "BTREE_SHARDS=${BTREE_SHARDS}"
        "BTREE_DURATION_SEC=${BTREE_DURATION_SEC}"
        "BTREE_KEY_SPACE=${BTREE_KEY_SPACE}"
        "BTREE_HOT_PCT=${BTREE_HOT_PCT}"
        "BTREE_READ_PCT=${read_pct}"
        "BTREE_WRITE_PCT=${write_pct}"
        "BTREE_DELETE_PCT=${delete_pct}"
        "BTREE_SCAN_PCT=${scan_pct}"
        "BTREE_THREADS=${threads}"
        "BTREE_MEASURE_LATENCY=${BTREE_MEASURE_LATENCY}"
        "BTREE_LATENCY_SAMPLES_PER_THREAD=${BTREE_LATENCY_SAMPLES_PER_THREAD}"
        "BTREE_ZLIB_STREAM_CACHE=${BTREE_ZLIB_STREAM_CACHE}"
        "BTREE_BG_COMPACTION=${bg_compaction}"
        "BTREE_BG_THREADS=${BTREE_BG_THREADS}"
        "BTREE_BG_LANDING_HIGH_WATERMARK_PCT=${BTREE_BG_LANDING_HIGH_WATERMARK_PCT}"
        "BTREE_BG_SCAN_INTERVAL_US=${BTREE_BG_SCAN_INTERVAL_US}"
        "BTREE_BG_QUEUE_CAPACITY=${BTREE_BG_QUEUE_CAPACITY}"
        "BTREE_BG_CODEC=${BTREE_BG_CODEC}"
    )
    if [[ -n "${preload}" ]]; then
        env_args+=("LD_PRELOAD=${preload}")
    fi

    if ! run_with_optional_cpu_metrics "${log_file}" "${cpu_file}" env "${env_args[@]}" "${MIXED_BIN}"; then
        echo "failed: ${run_label} ${workload} threads=${threads}; see ${log_file}" >&2
        exit 1
    fi
    append_mixed_summary "${run_label}" "${workload}" "${threads}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${log_file}" "${landing_bytes}" "${bg_compaction}" "${BTREE_BG_THREADS}" "${BTREE_BG_LANDING_HIGH_WATERMARK_PCT}" "${BTREE_BG_CODEC}" "${cpu_file}"
}

run_workload_matrix_for_codec() {
    local run_label="$1"
    local qpl_path="$2"
    local qpl_mode="$3"
    local zlib_backend="$4"
    local preload="$5"
    local codec_filter="$6"

    local bg_values="${BTREE_BG_COMPACTION}"
    if [[ "${RUN_BG_SWEEP}" == "1" ]]; then
        bg_values="${BG_SWEEP_VALUES}"
    fi

    for landing_bytes in ${LANDING_LIST}; do
        for bg_compaction in ${bg_values}; do
            for threads in ${THREADS_LIST}; do
                run_mixed_case "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "read_only" 100 0 0 0 "${threads}" "${landing_bytes}" "${bg_compaction}" "${codec_filter}"
                run_mixed_case "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "write_only" 0 100 0 0 "${threads}" "${landing_bytes}" "${bg_compaction}" "${codec_filter}"
                run_mixed_case "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "read_write_8_2" 80 20 0 0 "${threads}" "${landing_bytes}" "${bg_compaction}" "${codec_filter}"
                run_mixed_case "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "read_write_5_5" 50 50 0 0 "${threads}" "${landing_bytes}" "${bg_compaction}" "${codec_filter}"
                run_mixed_case "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "read_write_2_8" 20 80 0 0 "${threads}" "${landing_bytes}" "${bg_compaction}" "${codec_filter}"
            done
        done
    done
}

run_tail_latency() {
    local run_label="$1"
    local qpl_path="$2"
    local qpl_mode="$3"
    local zlib_backend="$4"
    local preload="$5"
    local log_file="${RUN_DIR}/${run_label}_tail_latency.log"
    local csv_file="${RUN_DIR}/${run_label}_tail_latency.csv"

    echo "tail_latency run=${run_label} qpl=${qpl_path}/${qpl_mode} zlib=${zlib_backend}"
    local env_args=(
        "BTREE_QPL_PATH=${qpl_path}"
        "BTREE_QPL_MODE=${qpl_mode}"
        "TAIL_LATENCY_USE_SILESIA=${BTREE_USE_SILESIA}"
        "TAIL_LATENCY_KEY_COUNT=${TAIL_LATENCY_KEY_COUNT}"
        "TAIL_LATENCY_SAMPLE_COUNT=${TAIL_LATENCY_SAMPLE_COUNT}"
        "TAIL_LATENCY_WARMUP=${TAIL_LATENCY_WARMUP}"
        "TAIL_LATENCY_CSV=${csv_file}"
    )
    if [[ -n "${preload}" ]]; then
        env_args+=("LD_PRELOAD=${preload}")
    fi

    if ! env "${env_args[@]}" "${TAIL_BIN}" 2>&1 | tee "${log_file}"; then
        echo "tail latency failed: ${run_label}; see ${log_file}" >&2
        exit 1
    fi
    append_tail_latency_summary "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${log_file}" "${csv_file}"
}

run_config() {
    local run_label="$1"
    local qpl_path="$2"
    local qpl_mode="$3"
    local zlib_backend="$4"
    local preload="$5"

    if [[ "${run_label}" == "software_fixed" ]]; then
        for codec_filter in ${SOFTWARE_CODECS}; do
            run_workload_matrix_for_codec "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "${codec_filter}"
        done
    elif [[ "${run_label}" == qpl_iaa_* ]]; then
        for codec_filter in ${IAA_CODECS}; do
            run_workload_matrix_for_codec "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "${codec_filter}"
        done
    elif [[ "${run_label}" == "zlib_accel" ]]; then
        for codec_filter in ${ZLIB_ACCEL_CODECS}; do
            run_workload_matrix_for_codec "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "${codec_filter}"
        done
    else
        run_workload_matrix_for_codec "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}" "all"
    fi

    if [[ "${RUN_TAIL_LATENCY}" == "1" ]]; then
        run_tail_latency "${run_label}" "${qpl_path}" "${qpl_mode}" "${zlib_backend}" "${preload}"
    fi
}

echo "results: ${RUN_DIR}"
echo "software baseline: LZ4 CPU and QPL software fixed"
run_config "software_fixed" "software" "fixed" "none" ""

if [[ "${RUN_IAA}" == "1" ]]; then
    echo "IAA QPL fixed"
    run_config "qpl_iaa_fixed" "hardware" "fixed" "none" ""

    if [[ "${RUN_QPL_DYNAMIC}" == "1" ]]; then
        echo "IAA QPL dynamic"
        run_config "qpl_iaa_dynamic" "hardware" "dynamic" "none" ""
    fi
fi

if [[ "${RUN_ZLIB_ACCEL}" == "1" ]]; then
    echo "zlib-accel preload: ${ZLIB_ACCEL_SO}"
    run_config "zlib_accel" "software" "fixed" "zlib_accel_preload" "${ZLIB_ACCEL_SO}"
fi

echo "summary: ${SUMMARY}"
column -t -s $'\t' "${SUMMARY}" || cat "${SUMMARY}"
if [[ "${RUN_TAIL_LATENCY}" == "1" ]]; then
    echo "tail latency summary: ${TAIL_SUMMARY}"
    column -t -s $'\t' "${TAIL_SUMMARY}" || cat "${TAIL_SUMMARY}"
fi
