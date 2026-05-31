import great_expectations as gx

print("Initializing Great Expectations Data Context...")
# This completely replaces the old 'init' command and creates your folder structure
context = gx.get_context(mode="file")
print("Success! Your GX environment is ready.")