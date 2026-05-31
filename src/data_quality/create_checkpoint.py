import great_expectations as gx

def run_enterprise_validation():
    print("🚀 Waking up Border Patrol...")
    
    # Load the persistent context from the gx/ folder
    context = gx.get_context()
    checkpoint_name = "daily_gold_validation"

    # 1. Safely retrieve or create the checkpoint
    if checkpoint_name not in context.checkpoints:
        print(f"⚙️ Checkpoint '{checkpoint_name}' not found in gx/checkpoints. Building it now...")
        context.add_or_update_checkpoint(
            name=checkpoint_name,
            validations=[
                {
                    "batch_request": context.get_datasource("gold_warehouse").get_asset("fraud_velocity").build_batch_request(),
                    "expectation_suite_name": "gold_fraud_rules",
                }
            ]
        )
        print("✅ Checkpoint built and saved!")

    # 2. Run the Checkpoint
    print(f"🏃 Running '{checkpoint_name}' against the Gold Layer...")
    checkpoint = context.checkpoints.get(checkpoint_name)
    result = checkpoint.run()

    # 3. Open the Data Docs automatically
    print("📊 Generating Enterprise Data Docs...")
    context.open_data_docs()

    # 4. Final Output
    if result.success:
        print("✅ SUCCESS: Data Quality checks passed! The Gold Layer is certified.")
    else:
        print("❌ FAILED: Data Quality checks failed. Corrupted data detected.")

if __name__ == "__main__":
    run_enterprise_validation()