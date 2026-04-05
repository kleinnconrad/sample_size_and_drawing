[![xy](https://github.com/kleinnconrad/sample_size_and_drawing/actions/workflows/deploy_bundle.yml/badge.svg)](https://github.com/kleinnconrad/sample_size_and_drawing/actions/workflows/deploy_bundle.yml)

# Delta Table Sampling Utilities

This repository provides Python utilities packaged as a **Databricks Asset Bundle (DAB)** to calculate statistically significant sample sizes and extract unbiased data subsets using PySpark.

## Table of Contents
1. [Overview](#overview)
2. [Repository Structure](#repository-structure)
3. [Sample Size Calculator (Cochran)](#sample-size-calculator-cochran)
4. [Bernoulli Sampling](#bernoulli-sampling)
5. [Deployment & Execution](#deployment--execution)
6. [CI/CD Pipeline Architecture](#cicd-pipeline-architecture)

---

## Overview
Arbitrary sampling (e.g., `LIMIT 10000`) often creates biased, needlessly large, or statistically weak datasets. These utilities solve this by providing mathematical rigor and true randomization, optimizing cloud compute costs while ensuring reliable data products for ML, A/B testing, and analytics.

By leveraging Databricks Asset Bundles, these utilities are structured for seamless, reproducible deployments across Databricks Serverless environments, defining infrastructure and code as a single deployable unit.

---

## Repository Structure
This project is structured as a deployable bundle, separating the core Python logic from the Databricks Job definitions.

* **`databricks.yml`**: The core bundle configuration defining the project scope and workspace targets.
* **`resources/`**: Contains the YAML definitions for the Serverless Databricks Jobs.
  * `sample_size_job.yml`: Deploys the Cochran calculation workflow.
  * `sample_drawing.yml`: Deploys the Bernoulli sampling workflow.
* **`src/`**: Contains the core execution scripts (`cochran.py`, `bernoulli_sampling.py`).

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

## Deployment & Execution

Both the Cochran Calculator and the Bernoulli Sampler are designed to run as serverless Databricks Jobs via the Databricks CLI. 

### Prerequisites
Ensure you have the Databricks CLI installed and authenticated with your target workspace:
```bash
databricks auth login --host "https://<your-workspace-url>"
```

### 1. Deployment
Validate your bundle configuration syntax and deploy the code and resources to your workspace.

```Bash
# Validate the YAML configurations
databricks bundle validate
```

# Deploy the bundle to the active target
```Bash
databricks bundle deploy
```

### 2. Execution
Once deployed, you can trigger the individual jobs directly from your terminal. The scripts dynamically query the active metastore to fetch exact table row counts and execute the mathematically rigorous sampling logic.

```Bash
# Run the Cochran sample size calculation
databricks bundle run cochran_sample_size
```

# Run the Bernoulli sampling process
```Bash
databricks bundle run bernoulli_sample_drawing
```

## CI/CD Pipeline Architecture

This repository utilizes GitHub Actions to automate the continuous integration and deployment (CI/CD) of the Databricks Asset Bundle. The pipeline ensures that any changes to the underlying statistical scripts or job configurations are rigorously validated and seamlessly deployed to the Databricks Serverless environment.

### Deployment Trigger
The automated workflow (`deploy_bundle.yml`) is triggered automatically upon a `push` to the `main` branch, specifically when modifications occur in critical paths:
* `src/**/*.py`: Core PySpark sampling logic (Cochran & Bernoulli).
* `resources/**/*.yml`: Databricks Job configurations and parameter mappings.
* `databricks.yml`: The overarching asset bundle infrastructure definition.

### Workflow Steps
The pipeline executes on a headless Ubuntu runner and orchestrates the following steps:
1. **Source Checkout:** Retrieves the latest committed code.
2. **CLI Initialization:** Installs the official Databricks CLI (`setup-cli@main`) to enable bundle commands.
3. **Bundle Validation:** Runs `databricks bundle validate` to verify the syntactic and structural integrity of the YAML configurations and file paths before attempting infrastructure changes.
4. **Target Deployment:** Executes `databricks bundle deploy` to push the updated Python notebooks and job definitions to the Databricks workspace.

### Security & Authentication
Authentication with the Databricks environment is handled securely via GitHub Actions Secrets. No credentials are hardcoded into the repository.
* **`DATABRICKS_HOST`**: The target Databricks Workspace URL.
* **`DATABRICKS_TOKEN`**: A scoped Personal Access Token (PAT) authorizing the headless deployment process.
