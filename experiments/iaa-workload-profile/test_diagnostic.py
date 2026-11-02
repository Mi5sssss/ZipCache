import csv, tempfile, unittest
from pathlib import Path
from diagnose import analyze

class CausalSelection(unittest.TestCase):
    def make(self,sequence,**kw):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'events.csv'
            with p.open('w') as f:
                w=csv.writer(f);w.writerow('sequence requests kind leaf hot base_usable active_usable raw_usable units bytes encoded_blocks'.split())
                for i,(kind,leaf) in enumerate(sequence,1):w.writerow([i,i,kind,leaf,0,512,0,1024,1,1024,1])
            return analyze(p,window=4,allowance=512,**kw)
    def test_future_popularity_does_not_select(self):
        d=self.make([('G','a')]*3+[('G','b')]+[('G','b')]*4)
        self.assertEqual(d['windows'][0]['selected_leaf_ids'],['a'])
        self.assertEqual(d['heldout_avoidable_cold_gets'],0)
        self.assertFalse(d['gate_pass'])
    def test_cost_and_conversion_are_charged(self):
        d=self.make([('G','a')]*3+[('G','b')]+[('G','a')]*4)
        w=d['windows'][0]
        self.assertEqual(w['retained_increment_bytes'],512)
        self.assertEqual(w['initial_conversion_decode_jobs'],1)
        self.assertEqual(w['queued_plus_running_destinations_bound_bytes'],36864)
        self.assertTrue(d['gate_pass'])
    def test_raw_and_delta_hits_are_not_savings(self):
        d=self.make([('A','a')]*3+[('B','b')]+[('G','a')]*4)
        self.assertEqual(d['windows'][0]['selected_leaves'],0)
        self.assertFalse(d['gate_pass'])
if __name__=='__main__':unittest.main()
