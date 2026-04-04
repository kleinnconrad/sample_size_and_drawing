# Databricks notebook source
import math

Z_SCORES = {
    "90": 1.645, "95": 1.960, "98": 2.326, "99": 2.576
}

def get_delta_population_size(spark, catalog, schema, table):
    full_table_name = f"{catalog}.{schema}.{table}"
    try:
        print(f"Counting rows in {full_table_name}...")
        return spark.table(full_table_name).count()
    except Exception as e:
        print(f"Error accessing table '{full_table_name}': {e}")
        return None

def main():
    print("=== Delta Table Sample Size Calculator (Cochran) ===")
    
    # 1. Define Databricks Widgets (Parameters)
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "default")
    dbutils.widgets.text("table", "test_population")
    dbutils.widgets.text("error", "0.05")
    dbutils.widgets.text("proportion", "0.5")
    dbutils.widgets.text("confidence", "95")
    
    # 2. Retrieve values from Widgets
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    table_name = dbutils.widgets.get("table")
    e = float(dbutils.widgets.get("error"))
    p = float(dbutils.widgets.get("proportion"))
    conf_input = dbutils.widgets.get("confidence")

    z = Z_SCORES.get(conf_input, 1.960)

    # Note: 'spark' is automatically injected into notebook tasks
    N = get_delta_population_size(spark, catalog, schema, table_name)
    if N is None:
        return 
        
    print(f"\nTotal population (N) in table: {N:,} rows")

    if N == 0:
        print("The table is empty. Required sample size is 0.")
        return

    q = 1.0 - p
    n0 = ((z**2) * p * q) / (e**2)
    n = n0 / (1 + ((n0 - 1) / N))
    final_sample_size = math.ceil(n)
    
    print("\n" + "="*40)
    print("            RESULTS")
    print("="*40)
    print(f"Calculated Sample Size: {final_sample_size:,} rows")
    
    fraction = min(final_sample_size / N, 1.0)
    
    print("\nTo extract this sample directly into a DataFrame, run the following PySpark code in your next cell:")
    print("-" * 80)
    print(f"sample_df = spark.table('{catalog}.{schema}.{table_name}').sample(withReplacement=False, fraction={fraction:.6f}, seed=42)")
    print("display(sample_df)")
    print("-" * 80)

if __name__ == "__main__":
    main()