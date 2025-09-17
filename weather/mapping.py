"""This is a module describing possible weather values."""

import numpy as np


ANNUAL_AVG_TEMP = 10
AMP = 15  # max difference from annual avg base
PERIOD = 365
COUNT_OF_DAYS = PERIOD * 5

# time axis
t = np.arange(COUNT_OF_DAYS)

# seasonality
seasonality = AMP * np.sin(2 * np.pi * t / PERIOD + np.deg2rad(-90))

np.random.seed(42)

# adding variability
variability = np.random.normal(loc=3, scale=5, size=COUNT_OF_DAYS)

# calculating temperature
temperature = ANNUAL_AVG_TEMP + seasonality + variability

# creating dict
print(t)
print(temperature)
