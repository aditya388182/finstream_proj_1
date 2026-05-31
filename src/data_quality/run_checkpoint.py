import great_expectations as gx

def run_enterprise_validation():
    print("🚀 Waking up Border Patrol...")
    
    # Load the persistent context (initialized by 0.18.19)
    context = gx.get_context()
    datasource_name = "gold_warehouse"
    asset_name = "fraud_velocity_asset"
    suite_name = "gold_fraud_rules"
    checkpoint_name = "daily_gold_validation"

    # 1. Safely Register the Datasource AND the Asset
    try:
        datasource = context.get_datasource(datasource_name)
        # The connection exists, but let's make sure the table asset exists too
        try:
            asset = datasource.get_asset(asset_name)
        except LookupError:
            print(f"⚙️ Asset missing. Attaching '{asset_name}' to Datasource...")
            datasource.add_table_asset(name=asset_name, table_name="fraud_velocity")
    except ValueError:
        print("⚙️ Registering DuckDB Datasource...")
        datasource = context.sources.add_sql(
            name=datasource_name, 
            connection_string="duckdb:///gold_analytics/finstream_gold.duckdb"
        )
        datasource.add_table_asset(name=asset_name, table_name="fraud_velocity")

    # 2. Safely Build the Expectation Suite if missing
    try:
        suite = context.get_expectation_suite(suite_name)
    except gx.exceptions.DataContextError:
        print("⚙️ Building and saving Expectation Suite...")
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
        
        asset = context.get_datasource(datasource_name).get_asset(asset_name)
        batch_request = asset.build_batch_request()
        validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
        
        # Apply the enterprise rules
        validator.expect_column_values_to_not_be_null(column="user_id")
        validator.expect_column_values_to_be_between(column="swipe_count", min_value=1)
        validator.expect_column_values_to_be_in_set(
            column="risk_flag", 
            value_set=["NORMAL", "HIGH_VELOCITY_RISK", "HIGH_VALUE_RISK"]
        )
        validator.save_expectation_suite(discard_failed_expectations=False)

    # 3. Configure the Checkpoint
    batch_request = context.get_datasource(datasource_name).get_asset(asset_name).build_batch_request()
    context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ]
    )

    # 4. Execute the Validation
    print(f"🏃 Running '{checkpoint_name}' against the Gold Layer...")
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)

    # 5. Launch the UI
    print("📊 Generating Enterprise Data Docs...")
    context.open_data_docs()

    if result.success:
        print("✅ SUCCESS: Data Quality checks passed! The Gold Layer is certified.")
    else:
        print("❌ FAILED: Data Quality checks failed. Corrupted data detected.")

if __name__ == "__main__":
    run_enterprise_validation()
