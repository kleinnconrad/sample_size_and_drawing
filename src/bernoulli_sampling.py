def main():
    print("=== Delta Table Bernoulli Sampler ===")
    
    # 1. Gather user inputs
    catalog = input("Enter Catalog name: ").strip()
    schema = input("Enter Schema name: ").strip()
    table_name = input("Enter Table name: ").strip()
    
    try:
        target_sample_size = int(input("Enter desired sample size (number of rows): ").strip())
        if target_sample_size <= 0:
            print("Error: Sample size must be greater than 0.")
            return
    except ValueError:
        print("Error: Please enter a valid integer for the sample size.")
        return

    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    # 2. Query total population size (N)
    try:
        print(f"\nCalculating total row count for '{full_table_name}'...")
        total_rows = spark.table(full_table_name).count()
        print(f"Total rows in table: {total_rows:,}")
    except Exception as e:
        print(f"Error accessing table. Please check permissions and spelling.\nDetails: {e}")
        return

    # 3. Handle edge cases
    if total_rows == 0:
        print("The source table is empty.")
        return
        
    if target_sample_size >= total_rows:
        print(f"Target sample size ({target_sample_size:,}) is greater than or equal to the total table size.")
        print("Returning the full table.")
        fraction = 1.0
    else:
        # Calculate the Bernoulli probability fraction (p = n / N)
        fraction = target_sample_size / total_rows

    # 4. Perform the sampling and output the PySpark code
    print("\n" + "="*50)
    print("               SAMPLING DETAILS")
    print("="*50)
    print(f"Target Sample Size: {target_sample_size:,}")
    print(f"Calculated Fraction: {fraction:.8f} ({fraction * 100:.6f}%)")
    print("Note: Because this is true Bernoulli sampling, the exact final row count may vary slightly.")
    
    print("\nRun the following PySpark code in your next cell to materialize the sample:")
    print("-" * 80)
    print(f"sample_df = spark.table('{full_table_name}').sample(withReplacement=False, fraction={fraction:.8f}, seed=42)")
    print("display(sample_df)")
    print(f"print(f'Actual sampled row count: {{sample_df.count():,}}')")
    print("-" * 80)

if __name__ == "__main__":
    main()
  
