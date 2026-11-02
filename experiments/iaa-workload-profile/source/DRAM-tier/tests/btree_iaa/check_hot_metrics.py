"""Numerical/accounting regressions: extra memory, exact traces, missing evidence."""
import copy
import unittest
from hot_metrics import enrich


def row(codec, owned, peak, policy=0):
    return dict(case=f'{codec}-{policy}',codec=codec,policy=policy,status='ok',mode='audit',repeat=0,
        INPUT={'trace_sha256':'same'},CONFIG={'cpu_list':'0,1'},
        PRODUCT=dict(status='ok',mismatches=0,live_keys=10,live_kv_bytes=1000,
                     trace_hash='same',resident_usable=owned,audit=1,audit_peak_usable=peak),
        MEMORY_V3=dict(base_raw_record_bytes=1000,base_encoded_bytes=400,
                       base_format_bytes=100,qpl_private_memory_verified=codec!='qpl'),
        AGGREGATION=dict(stack_allocated_usable=100,layout=3))


class Metrics(unittest.TestCase):
    def test_data_is_not_total_memory(self):
        raw=row('raw',1000,1100); c=row('lz4',700,800)
        enrich([raw,c]); m=c['CAPACITY_METRICS']
        self.assertEqual(m['cold_data_ratio'],2.5)
        self.assertEqual(m['cold_framed_ratio'],2.0)
        self.assertAlmostEqual(m['memory_saved_retained_pct'],100*(1-800/1100))
        self.assertAlmostEqual(m['memory_saved_peak_pct'],25)
        self.assertEqual(m['capacity_gate'],'pass-tracked-scope')

    def test_best_raw_and_negative_saving(self):
        raw=row('raw',1000,1200); hot_raw=row('raw',1200,1100,1); c=row('lz4',1300,1400)
        enrich([raw,hot_raw,c]);m=c['CAPACITY_METRICS']
        self.assertEqual(m['raw_reference_retained_case'],'raw-0')
        self.assertEqual(m['raw_reference_peak_case'],'raw-1')
        self.assertLess(m['memory_saved_retained_pct'],0)
        self.assertEqual(m['capacity_gate'],'fail-tracked-scope')

    def test_different_trace_and_config_cannot_match(self):
        for field in ('INPUT','CONFIG'):
            raw=row('raw',1000,1100);c=row('lz4',700,800)
            c[field]['different']=True;enrich([raw,c])
            self.assertIsNone(c['CAPACITY_METRICS']['memory_saved_retained_pct'])

    def test_missing_peak_is_not_zero_or_pass(self):
        raw=row('raw',1000,1100);c=row('lz4',700,0);c['PRODUCT']['audit']=0
        enrich([raw,c]);m=c['CAPACITY_METRICS']
        self.assertIsNone(m['memory_saved_peak_pct'])
        self.assertEqual(m['capacity_gate'],'unavailable')

    def test_qpl_incomplete_and_surrogate(self):
        raw=row('raw',1000,1100);q=row('qpl',600,700);s=row('surrogate',600,700)
        enrich([raw,q,s])
        self.assertIsNotNone(q['CAPACITY_METRICS']['tracked_memory_saved_retained_pct'])
        for r in (q,s):
            self.assertIsNone(r['CAPACITY_METRICS']['memory_saved_retained_pct'])
            self.assertEqual(r['CAPACITY_METRICS']['capacity_gate'],'unavailable')
        self.assertIsNone(s['CAPACITY_METRICS']['cold_data_ratio'])

    def test_failure_and_all_hot(self):
        raw=row('raw',1000,1100);c=row('lz4',700,800)
        c['status']='failed';enrich([raw,c])
        self.assertIsNone(c['CAPACITY_METRICS']['cold_data_ratio'])
        c['status']='ok';c['MEMORY_V3']['base_raw_record_bytes']=0;c['MEMORY_V3']['base_encoded_bytes']=0
        enrich([raw,c]);self.assertIsNone(c['CAPACITY_METRICS']['cold_data_ratio'])


if __name__=='__main__':unittest.main()
