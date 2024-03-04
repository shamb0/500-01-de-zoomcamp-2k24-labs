#!/bin/bash

# Function to print usage
print_usage() {
    echo "Usage: $0 --type <type> --year <year> --month <month> --destination <destination>"
}

# Function to check if a directory exists
function check_dir_exists() {
  local dir_path="$1"
  if [[ ! -d "$dir_path" ]]; then
    echo "Directory '$dir_path' does not exist. Creating..."
    mkdir -p "$dir_path"
  fi
}

# Function to confirm overwrite
function confirm_overwrite() {
  local file_path="$1"
  if [[ -f "$file_path" ]]; then
    read -p "File '$file_path' already exists. Overwrite (y/N)? " -r answer
    if [[ ! $answer =~ ^[Yy]$ ]]; then
      echo "Download cancelled."
      exit 1
    fi
  fi
}

# Parse script arguments
type=""
year=""
month=""
destination=""

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --type)
      type="$2"
      shift 2 ;;
    --year)
      year="$2"
      shift 2 ;;
    --month)
      month="$2"
      shift 2 ;;
    --destination)
      destination="$2"
      shift 2 ;;
    *)
      echo "Invalid argument: '$key'"
      exit 1 ;;
  esac
done

# Validate required arguments
if [ -z "$type" ] || [ -z "$year" ] || [ -z "$month" ] || [ -z "$destination" ]; then
    echo "Error: All parameters are required."
    print_usage
    exit 1
fi

# Construct dataset URL
month=$(echo "$month" | awk '{print tolower($0)}') # convert month to lowercase
month_num=$(date -d "$month 1 2000" +%m) # convert month name to number
if [ -z "$month_num" ]; then
    echo "Invalid month name: $month"
    exit 1
fi

# Construct URL & destination filename
filename="${type}_tripdata_${year}-${month_num}.csv.gz"
url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/${type}/${filename}"

# Check and create destination directory
check_dir_exists "$destination"

# Confirm overwrite
destination_file="$destination/$filename"
confirm_overwrite "$destination_file"

# Download the file
echo "Downloading $filename to $destination..."
wget -O "$destination_file" -q --show-progress "$url"

echo "Download completed successfully."
