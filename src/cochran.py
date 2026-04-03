import math

# Z-score mapping for standard confidence intervals
Z_SCORES = {
    "90": 1.645,
    "95": 1.960,
    "98": 2.326,
    "99": 2.576
}

def get_delta_population_size(catalog, schema, table):
    """Queries the Databricks environment to get the exact row count of the Delta table."""
    full_table_name = f"{catalog}.{schema}.{table}"
    try:
        print(f"Counting rows in {full_table_name}...")
        # 'spark' is natively available in Databricks notebooks
        row_count = spark.table(full_table_name).count()
        return row_count
    except Exception as e:
        print(f"Error accessing table '{full_table_name}'. Make sure the table exists and you have read permissions.")
        print(f"Details: {e}")
        return None

def main():
    print("=== Delta Table Sample Size Calculator (Cochran) ===")
    
    # 1. Ask user for Databricks table coordinates
    catalog = input("Enter Catalog name: ").strip()
    schema = input("Enter Schema name: ").strip()
    table_name = input("Enter Table name: ").strip()
    
    # 2. Ask user for statistical parameters
    try:
        e = float(input("Enter max tolerated error as a decimal (e.g., 0.05 for 5%): ").strip())
        p = float(input("Enter expected value/proportion as a decimal (e.g., 0.5): ").strip())
        conf_input = input("Enter confidence interval (90, 95, 98, or 99): ").strip()
    except ValueError:
        print("Error: Invalid numerical input. Please enter numbers/decimals where requested.")
        return

    # Assign Z-score
    if conf_input not in Z_SCORES:
        print("Warning: Unsupported confidence interval entered. Defaulting to 95%.")
        z = Z_SCORES["95"]
    else:
        z = Z_SCORES[conf_input]

    # 3. Retrieve Population Size (N)
    N = get_delta_population_size(catalog, schema, table_name)
    if N is None:
        return # Exit if table access fails
        
    print(f"\nTotal population (N) in table: {N:,} rows")

    # 4. Cochran's Calculation
    if N == 0:
        print("The table is empty. Required sample size is 0.")
        return

    q = 1.0 - p
    
    # Base formula for infinite population
    n0 = ((z**2) * p * q) / (e**2)
    
    # Finite population correction
    n = n0 / (1 + ((n0 - 1) / N))
    
    final_sample_size = math.ceil(n)
    
    # 5. Output Results and PySpark Snippet
    print("\n" + "="*40)
    print("               RESULTS")
    print("="*40)
    print(f"Calculated Sample Size: {final_sample_size:,} rows")
    
    # Calculate the fraction needed for the PySpark .sample() method
    fraction = min(final_sample_size / N, 1.0)
    
    print("\nTo extract this sample directly into a DataFrame, run the following PySpark code in your next cell:")
    print("-" * 80)
    print(f"sample_df = spark.table('{catalog}.{schema}.{table_name}').sample(withReplacement=False, fraction={fraction:.6f}, seed=42)")
    print("display(sample_df)")
    print("-" * 80)

if __name__ == "__main__":
    main()
      
