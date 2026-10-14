"""Statistics unit test. Fabricated ratios are never performance evidence."""
from split_compare import interval,verdict
def ci(x):return interval([(s,x) for s in (11,29,47) for _ in range(3)])
assert verdict(ci(.7),ci(1),ci(1.05),ci(1.05))=="pass"
assert verdict(ci(.7),ci(1),ci(1.2),ci(1.05))=="fail"
assert verdict(ci(1),ci(1),ci(1),ci(1))=="fail"
assert verdict({"low":.7,"high":.9},{"low":1,"high":1.1},{"low":1,"high":1.05},None)=="inconclusive"
assert interval([]) is None
print("paired intervals and gate boundaries passed; not hardware data")
