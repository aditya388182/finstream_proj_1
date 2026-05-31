import great_expectations as gx

# 1. Load your local file context
context = gx.get_context(mode="file")

# 2. FIX: Use the complete absolute path to your root pipeline directory
connection_string = "duckdb:////Users/adityagurematti/Downloads/finstream_pipeline/gold_analytics/finstream_gold.duckdb"

print("Registering FinStream Gold DuckDB Data Source via absolute path...")

# 3. Add the SQL data source link
datasource = context.data_sources.add_sql(
    name="gold_warehouse", 
    connection_string=connection_string
)

# 4. Bind the table target
table_asset = datasource.add_table_asset(
    name="fraud_velocity_asset", 
    table_name="fraud_velocity"
)

print("✅ Enterprise Datasource successfully registered via absolute path!")