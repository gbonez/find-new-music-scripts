#!/bin/zsh
# load_env.sh - Load secrets.json into environment variables

# Check that jq is installed
if ! command -v jq &> /dev/null
then
    echo "❌ jq could not be found. Please install it (brew install jq)."
    exit 1
fi

# Load each key/value from secrets.json into environment variables
eval $(jq -r 'to_entries | map("export \(.key)=\"\(.value|tostring)\"") | .[]' secrets.json)

echo "✅ Loaded environment variables from secrets.json"
