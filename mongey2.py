import requests
import time
import csv
import sys
import os

# --- Configuration ---
USER_API_URL_TEMPLATE = "https://mcsrranked.com/api/users/{}"
# Input and Output file are the same now
DATA_CSV_PATH = 'mcsr_user_data.csv'
DELAY_SECONDS = 1.3                   # Delay between requests
MAX_RETRIES = 3                       # Max retries for errors (like 429)
RETRY_WAIT_SECONDS = 60               # How long to wait after a 429 error
CONSECUTIVE_ERROR_LIMIT = 10          # Stop if too many non-404 errors
# --- End Configuration ---


def get_user_data(uuid):
    """Fetches user data from the API for a given UUID."""
    url = USER_API_URL_TEMPLATE.format(uuid)
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, timeout=20)  # 20-second timeout

            if response.status_code == 200:
                try:
                    # Check if response is valid JSON
                    data = response.json().get('data', {})
                    return data, None  # Return data, no error
                # Catch JSONDecodeError (subclass of ValueError)
                except ValueError:
                    print(
                        f"\nInvalid JSON received for {uuid}. Status: {response.status_code}", file=sys.stderr)
                    return None, "Invalid JSON Response"

            elif response.status_code == 404:
                return None, 404  # User not found

            elif response.status_code == 429:
                print(
                    f"\nRate limit hit (429) for {uuid}. Waiting {RETRY_WAIT_SECONDS}s...", file=sys.stderr)
                time.sleep(RETRY_WAIT_SECONDS)
                retries += 1
                print(
                    f"Retrying {uuid} (Attempt {retries}/{MAX_RETRIES})...", file=sys.stderr)
                continue  # Retry the request

            else:
                # Other server-side or unexpected errors
                print(
                    f"\nAPI Error for {uuid}. Status: {response.status_code}", file=sys.stderr)
                return None, response.status_code

        except requests.exceptions.Timeout:
            print(f"\nTimeout Error for {uuid}", file=sys.stderr)
            # Allow retries for timeouts as well, could be temporary
            retries += 1
            if retries < MAX_RETRIES:
                print(
                    f"Retrying {uuid} after timeout (Attempt {retries}/{MAX_RETRIES})...", file=sys.stderr)
                time.sleep(5)  # Short delay before timeout retry
                continue
            else:
                print(
                    f"\nMax retries exceeded for {uuid} after timeout.", file=sys.stderr)
                return None, "Timeout Error (Retries Exceeded)"

        except requests.exceptions.RequestException as e:
            print(f"\nNetwork/Request Error for {uuid}: {e}", file=sys.stderr)
            # Don't retry immediately for general network errors, could be persistent
            return None, "Network Error"

        # If we fall through the loop (e.g., non-429 error initially)
        return None, response.status_code if 'response' in locals() else "Request Failed"

    # If retries exhausted specifically for 429
    print(
        f"\nMax retries exceeded for {uuid} after rate limit errors.", file=sys.stderr)
    return None, "Rate Limit Retries Exhausted"


# --- Main Script ---
all_user_data = []
original_headers = []
consecutive_errors = 0
update_count = 0

# 1. Read existing data
if not os.path.exists(DATA_CSV_PATH):
    print(
        f"Error: Data file '{DATA_CSV_PATH}' not found. Cannot update.", file=sys.stderr)
    sys.exit(1)

print(f"Reading existing data from {DATA_CSV_PATH}...")
try:
    with open(DATA_CSV_PATH, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        if not reader.fieldnames:
            print(
                f"Error: CSV file '{DATA_CSV_PATH}' appears empty or has no header.", file=sys.stderr)
            sys.exit(1)
        original_headers = reader.fieldnames
        # Ensure essential columns exist
        if 'uuid' not in original_headers or 'eloRate' not in original_headers:
            print("Error: CSV must contain 'uuid' and 'eloRate' columns.",
                file=sys.stderr)
            sys.exit(1)
        # Add 'status' column if it doesn't exist
        if 'status' not in original_headers:
            original_headers.append('status')

        all_user_data = list(reader)  # Read all rows into memory
except Exception as e:
    print(f"Error reading CSV file: {e}", file=sys.stderr)
    sys.exit(1)

total_users = len(all_user_data)
if total_users == 0:
    print("CSV file contains no data rows (only header potentially). Exiting.")
    sys.exit(0)

print(f"Found {total_users} users in the CSV.")
print(f"Updating data in-place in {DATA_CSV_PATH}")
print("-" * 30)

# --- Process and Update ---
try:
    for i, user_row in enumerate(all_user_data):
        uuid = user_row.get('uuid')
        if not uuid:
            print(
                f"\nWarning: Skipping row {i+1} due to missing UUID.", file=sys.stderr)
            # Add status if not present
            user_row['status'] = "Skipped (Missing UUID)"
            continue

        print(f"\rProcessing {i+1}/{total_users} ({uuid})...",
            end='', file=sys.stderr)
        sys.stderr.flush()  # Ensure progress message is displayed immediately

        fetched_data, error_code = get_user_data(uuid)
        # Keep original if fetch fails
        original_elo = user_row.get('eloRate', '')

        if fetched_data:
            consecutive_errors = 0
            new_elo = fetched_data.get('eloRate')
            new_elo_str = '' if new_elo is None else str(
                new_elo)  # Handle API returning null

            if user_row.get('eloRate') != new_elo_str:
                user_row['eloRate'] = new_elo_str
                user_row['status'] = "OK Updated"
                update_count += 1
            else:
                user_row['status'] = "OK (No Change)"

            # Optionally update nickname/twitch if they changed (check if columns exist first)
            if 'nickname' in original_headers:
                user_row['nickname'] = fetched_data.get(
                    'nickname', user_row.get('nickname', ''))
            if 'twitch_name' in original_headers:
                connections = fetched_data.get('connections', {})
                twitch_info = connections.get(
                    'twitch') if connections else None
                user_row['twitch_name'] = twitch_info.get('name', user_row.get(
                    'twitch_name', '')) if twitch_info else user_row.get('twitch_name', '')

        else:
            # Handle errors - Keep original Elo
            user_row['eloRate'] = original_elo  # Explicitly keep original
            if error_code == 404:
                user_row['status'] = "Not Found (404)"
                consecutive_errors = 0  # Reset for 404
            elif error_code == "Rate Limit Retries Exhausted":
                user_row['status'] = "Error (Rate Limit)"
                consecutive_errors += 1
            elif error_code == "Network Error":
                user_row['status'] = "Error (Network)"
                consecutive_errors += 1
            elif error_code == "Timeout Error (Retries Exceeded)":
                user_row['status'] = "Error (Timeout)"
                consecutive_errors += 1
            elif error_code == "Invalid JSON Response":
                user_row['status'] = "Error (Invalid JSON)"
                consecutive_errors += 1
            else:
                user_row['status'] = f"Error ({error_code})"
                consecutive_errors += 1

            print(
                f"\nIssue processing {uuid}: {user_row['status']}", file=sys.stderr)

        # Check for stopping due to errors
        if consecutive_errors >= CONSECUTIVE_ERROR_LIMIT:
            print(
                f"\nStopping due to {consecutive_errors} consecutive errors.", file=sys.stderr)
            break

        # Wait
        time.sleep(DELAY_SECONDS)

except KeyboardInterrupt:
    print("\nProcess interrupted by user.", file=sys.stderr)

finally:
    # --- Write Updated Data Back ---
    print("\n" + "-" * 30)
    print(f"Finished processing attempts.")
    print(f"Updated Elo for {update_count} users.")

    # Check if we have data (might be empty if interrupted early)
    if all_user_data:
        print(f"Saving updated data back to {DATA_CSV_PATH}...")
        try:
            with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                # Use the headers read initially (plus 'status' if added)
                writer = csv.DictWriter(
                    outfile, fieldnames=original_headers, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_user_data)  # Write the modified list
            print("Successfully saved updated data.")
        except IOError as e:
            print(
                f"\nError writing updated data to {DATA_CSV_PATH}: {e}", file=sys.stderr)
            print("Partial updates might be lost.")
    else:
        print("No data structure available to save (likely interrupted very early).")
