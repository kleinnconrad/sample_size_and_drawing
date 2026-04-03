# Delta Table Sampling Utilities

This repository provides Python utilities for Databricks environments to calculate statistically significant sample sizes and extract unbiased data subsets using PySpark.

## Table of Contents
1. [Overview](#overview)
2. [Sample Size Calculator (Cochran)](#sample-size-calculator-cochran)
3. [Bernoulli Sampling](#bernoulli-sampling)
4. [Using the Python Scripts](#using-the-python-scripts)

---

## Overview
Arbitrary sampling (e.g., `LIMIT 10000`) often creates biased, needlessly large, or statistically weak datasets. These utilities solve this by providing mathematical rigor and true randomization, optimizing cloud compute costs while ensuring reliable data products for ML, A/B testing, and analytics.

---

## Sample Size Calculator (Cochran)

Cochran's formula calculates the minimum required sample size based on your desired precision and confidence level. It works in two steps:

### 1. Base Sample Size (Infinite Population)
Assumes an infinitely large population.

$$n_0 = \frac{Z^2 p q}{e^2}$$

* **Z**: Z-value for confidence level (e.g., 1.96 for 95%).
* **p**: Estimated proportion (typically 0.5 for maximum variance).
* **q**: Remaining proportion (1 - p).
* **e**: Margin of error (e.g., 0.05 for 5%).

### 2. Finite Population Correction
Adjusts the required sample size downwards based on the actual, finite row count of your Delta table.

$$n = \frac{n_0}{1 + \frac{n_0 - 1}{N}}$$

* **n**: Final corrected sample size.
* **N**: Total population size (table row count).

---

## Bernoulli Sampling

Bernoulli sampling evaluates each row independently, giving it an equal probability (fraction) of being included—similar to flipping a weighted coin for every row. 

Because each selection is independent, the final sample size is approximate due to natural variance. This guarantees a truly randomized, unbiased subset across the entire dataset, which is a strict prerequisite for reliable machine learning and statistical testing.

---

## Using the Python Scripts

Both the Cochran Calculator and the Bernoulli Sampler share the same core mechanics and are designed for interactive Databricks notebooks.

### Key Features
* **Dynamic Metastore Querying:** Utilizes the active `spark` session to automatically fetch exact table row counts.
* **Intuitive Prompts:** Requests clear targets (like desired row counts or margins of error) rather than abstract probability fractions.
* **Reproducible Code Generation:** Calculates the necessary math and outputs a ready-to-run PySpark snippet (using `.sample()`) with a fixed seed to guarantee deterministic results.

### Quick Start
1. Paste either Python script into an empty Databricks notebook cell and run it.
2. Follow the input prompts for your catalog, schema, table name, and desired parameters.
3. Copy the generated PySpark code from the output.
4. Run the snippet in your next cell to materialize your statistically sound sample DataFrame.
   
