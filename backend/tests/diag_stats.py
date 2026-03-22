
import numpy as np
from scipy import stats

def check(a, b):
    t_res = stats.ttest_ind(a, b, equal_var=False)
    mw_res = stats.mannwhitneyu(a, b, alternative='two-sided')
    print(f"A: {a}, B: {b}")
    print(f"  T-test p: {t_res.pvalue}")
    print(f"  MW p:     {mw_res.pvalue}")
    print(f"  Min:      {min(t_res.pvalue, mw_res.pvalue)}")

print("Identical non-zero:")
check([1, 2, 3], [1, 2, 3])

print("\nIdentical zeros:")
check([0, 0, 0], [0, 0, 0])

print("\nOne different:")
check([1, 1, 1], [1, 1, 0])

print("\nOne extreme outlier:")
check([0, 0, 0, 0, 0], [0, 0, 0, 0, 1000])

print("\nVery small difference, large skew:")
check([0]*99 + [100], [0]*99 + [101])
