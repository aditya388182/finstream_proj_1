import great_expectations as gx

context = gx.get_context()

# 1. Create a new persistent suite
suite = context.add_expectation_suite(expectation_suite_name="gold_fraud_rules")

# 2. Get the data asset we registered in Step 2
datasource = context.get_datasource("gold_warehouse")
asset = datasource.get_asset("fraud_velocity")
batch_request = asset.build_batch_request()

# 3. Build the rules
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite.name
)

validator.expect_column_values_to_not_be_null(column="user_id")
validator.expect_column_values_to_be_between(column="swipe_count", min_value=1)
validator.expect_column_values_to_be_in_set(
    column="risk_flag", 
    value_set=["NORMAL", "HIGH_VELOCITY_RISK", "HIGH_VALUE_RISK"]
)

# 4. Save the rules to disk (JSON)
validator.save_expectation_suite(discard_failed_expectations=False)
print("✅ Expectation Suite successfully saved to disk!")