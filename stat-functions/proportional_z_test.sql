## Code for the proportional z-test function in snowflake
CREATE OR REPLACE FUNCTION discovery_sandbox.PROPORTIONAL_Z_TEST(sample1_success INT, sample1_size INT, sample2_success INT, sample2_size INT)
RETURNS VARIANT
LANGUAGE PYTHON
runtime_version = 3.8
packages = ('scipy','statsmodels')
handler = 'PROPORTIONAL_Z_TEST'
AS

$$
from statsmodels.stats.proportion import proportions_ztest
import scipy.stats as st
import numpy as np

def PROPORTIONAL_Z_TEST(sample1_success, sample1_size, sample2_success, sample2_size):

    count = np.array([sample1_success, sample2_success])
    nobs = np.array([sample1_size, sample2_size])
    z_score, p_value = proportions_ztest(count, nobs)

    # confidence interval
    pooled_prop = sum(count) / sum(nobs)
    se = pooled_prop * (1 - pooled_prop) * sum(1/nobs)
    confidence_interval = (pooled_prop - 1.96*se, pooled_prop + 1.96*se)

    return {'z_score': z_score, 'p_value': p_value, 'confidence_interval': confidence_interval}
$$;
commit;

