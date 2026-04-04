# Databricks notebook source
def main():
    print("=== Delta Table Bernoulli Sampler ===")
    
    # 1. Define Databricks Widgets (Parameters)
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "default")
    dbutils.widgets.text("table", "test_population")
    dbutils.widgets.text("target_size", "5")

    # 2. Retrieve values from Widgets
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    table_name = dbutils.widgets.get("table")
    target_sample_size = int(dbutils.widgets.get("target_size"))

    if target_sample_size <= 0:
        print("Error: Sample size must be greater than 0.")
        return

    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        print(f"\nCalculating total row count for '{full_table_name}'...")
        total_rows = spark.table(full_table_name).count()
        print(f"Total rows in table: {total_rows:,}")
    except Exception as e:
        print(f"Error accessing table. Please check permissions and spelling.\nDetails: {e}")
        return

    if total_rows == 0:
        print("The source table is empty.")
        return
        
    if target_sample_size >= total_rows:
        print(f"Target sample size ({target_sample_size:,}) is greater than or equal to the total table size.")
        fraction = 1.0
    else:
        fraction = target_sample_size / total_rows

    print("\n" + "="*50)
    print("               SAMPLING DETAILS")
    print("="*50)
    print(f"Target Sample Size: {target_sample_size:,}")
    print(f"Calculated Fraction: {fraction:.8f} ({fraction * 100:.6f}%)")
    
    print("\nRun the following PySpark code in your next cell to materialize the sample:")
    print("-" * 80)
    print(f"sample_df = spark.table('{full_table_name}').sample(withReplacement=False, fraction={fraction:.8f}, seed=42)")
    print("display(sample_df)")
    print("-" * 80)

if __name__ == "__main__":
    main()