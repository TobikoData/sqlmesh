#!/bin/bash

# Navigate to the migrations directory (modify the path if necessary)
cd "sqlmesh/migrations" || exit 1


# Collect all migration files matching the pattern (e.g., v0001_initial.py)
migration_files=(v*.py)

# Initialize an array to hold migration numbers
numbers=()

# Extract migration numbers from filenames
for file in "${migration_files[@]}"; do
  if [[ $file =~ ^v0*([0-9]+)_ ]]; then
    num=${BASH_REMATCH[1]}
    numbers+=("$num")
  fi
done

# Check if any migration files were found
if [[ ${#numbers[@]} -eq 0 ]]; then
  echo "No migration files found matching the pattern 'v<zero-padded number>_<description>.py'."
  exit 1
fi

# Check for duplicate migration numbers
duplicates=$(printf "%s\n" "${numbers[@]}" | sort | uniq -d)
if [[ -n $duplicates ]]; then
  echo "Error: Duplicate migration numbers found: $duplicates"
  exit 1
fi

# Sort the migration numbers
sorted_numbers=($(printf "%s\n" "${numbers[@]}" | sort -n))

# Get the first and last migration numbers
first_number="${sorted_numbers[0]}"
last_index=$((${#sorted_numbers[@]} - 1))
last_number="${sorted_numbers[$last_index]}"

# Check for gaps in the migration sequence
expected_numbers=($(seq "$first_number" "$last_number"))

if [[ "${sorted_numbers[*]}" != "${expected_numbers[*]}" ]]; then
  echo "Error: Missing migration numbers in sequence."
  echo "Expected sequence: ${expected_numbers[*]}"
  echo "Found sequence:    ${sorted_numbers[*]}"
  exit 1
fi

echo "All migration numbers are sequential and without overlaps."
exit 0
