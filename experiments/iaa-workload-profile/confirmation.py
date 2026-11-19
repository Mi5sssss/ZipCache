"""Paired tail latency and matched memory measurements for the selected GET mixes."""
import sys
from statistics import median


def confirmation_summary(performance, audits, reads, rate, hashes, device='qpl'):
    """Rows have already passed the benchmark's full-result validation."""
    def cases(rows, pct, codec, mode, repeats):
        point = f'uniform-r{pct}'
        selected = [r for r in rows if r['point'] == point and r['codec'] == codec]
        if sorted(r['repeat'] for r in selected) != list(range(repeats)):
            raise ValueError(f'{point}/{codec}: missing or duplicate {mode} result')
        for r in selected:
            e = r['EXECUTION']
            if (r['status'] != 'ok' or r['mode'] != mode or
                    r['INPUT']['trace_sha256'] != hashes.get(f"{point}-n{r['INPUT']['operations']}") or
                    e['arrival_rate'] != rate or e['latency_basis'] != 'scheduled-arrival-to-return'):
                raise ValueError(f'{point}/{codec}: invalid result, trace, arrival rate or latency clock')
            if codec == 'qpl' and not any(route['requested'] == 'hardware' and route['completed'] > 0
                                          for route in e['routes']):
                raise ValueError(f'{point}/{codec}: no hardware completions observed')
        return selected

    def fmt(value, decimals=2):
        return 'n/a' if value is None else f'{value:.{decimals}f}'

    def rss(product):
        # getrusage reports bytes on macOS and KiB on the Linux benchmark host.
        current = product.get('rss') or 0
        high = (product.get('ru_maxrss_native') or 0) * (1 if sys.platform == 'darwin' else 1024)
        return [v / 2**20 if v else None for v in (current, high)]

    timed = [f'Matched arrival rate: {rate} requests/s total. Medians of three runs; per-run p99 includes client backlog.',
             'GET target | codec | foreground QPS / QPS through drain | CPU ns/op | GET p99 us | PUT p99 us | process RSS at drain / high-water MiB']
    memory = ['Separate audit at the same offered rate. Retained = after drain. Owned peak excludes preload.',
              'GET target | codec | cold data ratio | tracked owned retained / peak MiB | tracked saving vs RAW retained / peak | audit process RSS at drain / high-water MiB | owned ledger complete']
    for pct in sorted(reads):
        for codec in ('lz4', device):
            products = [r['PRODUCT'] for r in cases(performance, pct, codec, 'performance', 3)]
            qps = median(p['operations'] * 1e9 / p['completed_ns'] for p in products)
            foreground = median(p['operations'] * 1e9 / p['foreground_ns'] for p in products)
            cpu = median(p['cpu_ns'] / p['operations'] for p in products)
            tails = [fmt(median(p[lat] / 1000 for p in products)) if products[0][count] else 'n/a'
                     for count, lat in [('reads','get_p99_ns'), ('writes','put_p99_ns')]]
            current, peak = zip(*(rss(p) for p in products))
            timed.append(f'{pct}% | {codec} | {foreground:.0f} / {qps:.0f} | {cpu:.0f} | ' + ' | '.join(tails) +
                         f' | {fmt(median(current) if all(current) else None)} / {fmt(median(peak) if all(peak) else None)}')
        for codec in ('raw', 'lz4', device):
            row, = cases(audits, pct, codec, 'audit', 1)
            s = row['STORAGE']
            current, peak = rss(row['PRODUCT'])
            retained, high = [s[f'total_owned_{k}_bytes'] for k in ('retained', 'peak')]
            saved = [s[f'tracked_memory_saved_{k}_pct'] for k in ('retained', 'peak')]
            if not retained or not high or None in saved:
                raise ValueError(f'{pct}%/{codec}: missing owned bytes or matched RAW')
            memory.append(f"{pct}% | {codec} | {fmt(s['cold_data_ratio'], 3)} | {retained/2**20:.2f} / {high/2**20:.2f} | "
                          f"{saved[0]:.2f}% / {saved[1]:.2f}% | {fmt(current)} / {fmt(peak)} | {s['memory_ledger_complete_for_scope']}")
    return timed + [''] + memory + [
        'Cold ratio excludes framing and hot RAW leaves. Tracked savings exclude unverified QPL-private memory.',
        'Process RSS includes fixtures, stacks and runtime; the high-water mark includes preload. Compare audit timings separately.']
