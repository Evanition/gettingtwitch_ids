import requests
import time
import csv
import sys
import os
import datetime
import traceback
import math

# --- Configuration ---
USER_API_URL_TEMPLATE = "https://mcsrranked.com/api/users/{}"
MATCHES_API_URL = "https://mcsrranked.com/api/matches"
DATA_CSV_PATH = 'mcsr_user_data.csv'       # Path for main user data CSV
# NEW: File to store the ID of the newest match seen
LAST_MATCH_ID_FILE = 'last_match_id.txt'

# API Request Delays
DELAY_MATCHES_SECONDS = 1.21              # Delay between match list API requests
# Delay between individual user API requests
DELAY_USER_SECONDS = 1.21

# Retry Mechanism
# Max retries for API errors (like 429, timeouts)
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 60                  # How long to wait after a 429 error
# Stop phase if this many consecutive API errors occur
CONSECUTIVE_API_ERROR_LIMIT = 5

# Fetching Logic
# NEW NAME: How many recent matches to try and fetch in THIS run
MAX_RECENT_MATCHES_TO_FETCH_PER_RUN = 10000
MATCHES_PER_PAGE = 100                   # Max allowed by API

# Update Logic for Existing Users
# Don't update user's full profile if scraped within this interval
UPDATE_INTERVAL_MINUTES = 30
# --- End Configuration ---


# --- Helper Functions ---
def parse_timestamp(timestamp_str):
    """Safely parses an ISO timestamp string (with optional Z) into a timezone-aware datetime object."""
    if not timestamp_str:
        return None
    try:
        if isinstance(timestamp_str, str) and timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        dt = datetime.datetime.fromisoformat(timestamp_str)
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def should_update_user(last_scraped_timestamp_str, update_interval_minutes):
    """Checks if a user should be updated based on the last scraped time."""
    last_scraped_dt = parse_timestamp(last_scraped_timestamp_str)
    if not last_scraped_dt:
        return True  # No valid timestamp, so update

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    time_since_last_update = now_utc - last_scraped_dt
    update_threshold = datetime.timedelta(minutes=update_interval_minutes)

    return time_since_last_update >= update_threshold


def get_api_data(url, params=None):
    """Generic function to fetch data from API with retries and robust error handling."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            # Updated user agent version
            headers = {'User-Agent': 'MCSRRankedDataUpdaterScript/1.3'}
            response = requests.get(
                url, params=params, headers=headers, timeout=25)

            if response.status_code == 200:
                try:
                    data = response.json()
                    # MCSR API /users/{id} returns JSON wrapped in status/data
                    if isinstance(data, dict) and data.get('status') == 'success':
                        return data.get('data'), None
                    # MCSR API /matches/ returns list directly, not wrapped
                    elif isinstance(data, list):
                        return data, None
                    # Fallback for unexpected structure but 200 OK
                    else:
                        err_msg = f"Unexpected JSON structure. Status: {data.get('status', 'N/A')}, Data Type: {type(data)}"
                        print(
                            f"\nAPI Logic Error at {url}. {err_msg}", file=sys.stderr)
                        return None, err_msg
                except requests.exceptions.JSONDecodeError:
                    print(
                        f"\nInvalid JSON received from {url}. Status: {response.status_code}, Response Text: {response.text[:100]}...", file=sys.stderr)
                    return None, "Invalid JSON Response"

            elif response.status_code == 404:
                return None, 404  # Not found (e.g., user doesn't exist)

            elif response.status_code == 429:
                print(
                    f"\nRate limit hit (429) for {url}. Waiting {RETRY_WAIT_SECONDS}s...", file=sys.stderr)
                time.sleep(RETRY_WAIT_SECONDS)
                retries += 1
                print(
                    f"Retrying {url} (Attempt {retries}/{MAX_RETRIES})...", file=sys.stderr)
                # Retry the request (don't increment consecutive errors yet)
                continue

            elif response.status_code == 400:
                print(
                    f"\nAPI returned 400 Bad Request for {url}. Params: {params}. Check if data exists or parameters are valid.", file=sys.stderr)
                try:
                    error_data = response.json()
                    if isinstance(error_data, dict) and error_data.get('status') == 'error':
                        print(
                            f"  API Error Message: {error_data.get('data')}", file=sys.stderr)
                        return None, f"API Error: {error_data.get('data')}"
                except requests.exceptions.JSONDecodeError:
                    pass  # Couldn't parse error JSON, just proceed
                return None, "HTTP 400"  # Return generic 400 if no specific message

            else:
                print(
                    f"\nAPI Error for {url}. Status: {response.status_code}", file=sys.stderr)
                return None, f"HTTP {response.status_code}"

        except requests.exceptions.Timeout:
            print(f"\nTimeout Error for {url}", file=sys.stderr)
            retries += 1
            if retries < MAX_RETRIES:
                print(
                    f"Retrying {url} after timeout (Attempt {retries}/{MAX_RETRIES})...", file=sys.stderr)
                time.sleep(5)  # Short delay before timeout retry
                continue
            else:
                print(
                    f"\nMax retries exceeded for {url} after timeout.", file=sys.stderr)
                return None, "Timeout Error (Retries Exceeded)"

        except requests.exceptions.RequestException as e:
            print(f"\nNetwork/Request Error for {url}: {e}", file=sys.stderr)
            # This is a broader error, let caller decide if it should count towards consecutive
            return None, "Network Error"

    # If retries exhausted
    print(
        f"\nMax retries exhausted for {url}. Giving up on this request.", file=sys.stderr)
    return None, "Retries Exhausted"


def read_last_match_id(filepath):
    """Reads the last known match ID from a file."""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                content = f.read().strip()
                if content:
                    return int(content)
        except (ValueError, IOError) as e:
            print(
                f"Warning: Could not read/parse {filepath}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
    return None  # No ID found or error


def write_last_match_id(filepath, match_id):
    """Writes the last known match ID to a file."""
    try:
        with open(filepath, 'w') as f:
            f.write(str(match_id))
    except IOError as e:
        print(f"Error writing to {filepath}: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)


# --- Main Script Logic (Runs Once) ---
run_start_time = datetime.datetime.now()
print(
    f"--- Starting Update Cycle at {run_start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

# --- Initialize variables for this run ---
# Stores UUID -> user_row_dict (for quick lookup and modification)
user_data_map = {}
# Define baseline headers for a potentially new/empty CSV.
# These are the columns we expect to manage.
original_headers = ['uuid', 'nickname', 'eloRate',
                    'twitch_name', 'status', 'last_scraped_at']

# Counters for summary
update_count_match = 0
update_count_twitch = 0
skipped_recent_count = 0
new_users_added_count = 0
processed_match_players = 0
# UUIDs that need a full profile fetch (for Twitch etc.)
uuids_to_fetch_full_profile = set()

# NEW: Variables for tracking newest match for next run
first_match_id_in_run = None  # Stores the ID of the newest match found in THIS run
last_run_match_id = None     # Stores the ID of the newest match from the PREVIOUS run

try:
    # --- 0. Read the last match ID from previous run ---
    last_run_match_id = read_last_match_id(LAST_MATCH_ID_FILE)
    print(
        f"Last match ID from previous run: {last_run_match_id if last_run_match_id else 'None (first run or file error)'}")

    # --- 1. Read existing user data OR create new CSV with headers ---
    # If the CSV file does not exist, create it with the baseline headers
    if not os.path.exists(DATA_CSV_PATH):
        print(
            f"Data file '{DATA_CSV_PATH}' not found. Creating a new one with baseline headers.")
        try:
            with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=original_headers)
                writer.writeheader()
            print(f"Created new CSV: {DATA_CSV_PATH}")
        except IOError as e:
            print(
                f"Error creating new CSV file '{DATA_CSV_PATH}': {e}. Exiting.", file=sys.stderr)
            sys.exit(1)

    # Now, DATA_CSV_PATH is guaranteed to exist (even if just created empty)
    print(f"Reading existing data from {DATA_CSV_PATH}...")
    try:
        with open(DATA_CSV_PATH, 'r', newline='', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)

            # If the CSV has no actual data rows (just headers or empty), use baseline headers
            if not reader.fieldnames:
                print(
                    f"CSV file '{DATA_CSV_PATH}' has no content. Using baseline headers.")
                # original_headers is already set to the baseline
            else:
                # If file has headers, use them and ensure all required/optional ones are present
                current_file_headers = list(reader.fieldnames)

                # Ensure all *required* headers are present
                required_cols = ['uuid', 'eloRate', 'nickname']
                missing_req = [
                    col for col in required_cols if col not in current_file_headers]
                if missing_req:
                    print(
                        f"Error: CSV must contain required columns: {', '.join(missing_req)}. Exiting.", file=sys.stderr)
                    sys.exit(1)

                # Add any *optional* headers if they are missing from the file's current headers
                for col in ['status', 'last_scraped_at', 'twitch_name']:
                    if col not in current_file_headers:
                        current_file_headers.append(col)
                # Update the global headers list based on file + additions
                original_headers = current_file_headers

            # Read rows and populate user_data_map
            # Keep original order and rows with missing UUIDs for now
            all_rows_from_csv = list(reader)
            temp_map = {}
            rows_with_missing_uuid = 0
            for i, user_row in enumerate(all_rows_from_csv):
                uuid = user_row.get('uuid')
                if uuid:
                    # Ensure all cells for existing rows are initialized for all original_headers
                    for col in original_headers:
                        user_row.setdefault(col, '')
                    temp_map[uuid] = user_row
                else:
                    rows_with_missing_uuid += 1
                    for col in original_headers:
                        user_row.setdefault(col, '')
                    # Mark rows that can't be processed
                    user_row['status'] = "Skipped (Missing UUID)"

            user_data_map = temp_map  # This map only contains valid UUIDs
            if rows_with_missing_uuid > 0:
                print(
                    f"Warning: {rows_with_missing_uuid} row(s) in CSV have missing UUID and will be excluded from updates.", file=sys.stderr)

    except Exception as e:
        print(
            f"Error reading/initializing CSV: {e}. Exiting.", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    valid_users_in_map = len(user_data_map)
    print(f"Loaded {valid_users_in_map} users with valid UUIDs from CSV.")

    print(
        f"Attempting to fetch up to {MAX_RECENT_MATCHES_TO_FETCH_PER_RUN} NEW matches (Elo/Nick) and full profiles (Twitch).")
    print("-" * 30)

    # --- 2. Update/Add Users from Recent Matches (Phase 1 - PAGINATED) ---
    print(f"Fetching matches (in pages of {MATCHES_PER_PAGE})...")
    matches_data_aggregated = []  # Store all matches here
    # Used for the 'before' cursor within this run
    last_match_id_for_pagination = None
    fetch_match_error = None
    pages_fetched = 0
    consecutive_match_api_errors = 0

    # Loop to fetch new matches using 'after' and paginate if needed
    while len(matches_data_aggregated) < MAX_RECENT_MATCHES_TO_FETCH_PER_RUN:
        pages_fetched += 1
        current_params = {'count': MATCHES_PER_PAGE}

        # If this is the first page of matches for THIS run, use 'after' to get new matches
        # Otherwise, use 'before' to paginate further back for more matches in this run
        if pages_fetched == 1 and last_run_match_id:
            current_params['after'] = last_run_match_id
            print(
                f"\rFetching match page {pages_fetched} (after ID: {last_run_match_id})...", end='', file=sys.stderr)
        elif last_match_id_for_pagination:
            current_params['before'] = last_match_id_for_pagination
            print(
                f"\rFetching match page {pages_fetched} (before ID: {last_match_id_for_pagination})...", end='', file=sys.stderr)
        else:  # First run, no previous ID to start after, and no 'before' yet
            print(
                f"\rFetching match page {pages_fetched} (latest matches)...", end='', file=sys.stderr)

        sys.stderr.flush()

        time.sleep(DELAY_MATCHES_SECONDS)  # Delay before each page request
        current_batch, match_error = get_api_data(
            MATCHES_API_URL, params=current_params)

        if match_error:
            consecutive_match_api_errors += 1
            print(
                f"\nError fetching match page {pages_fetched}: {match_error}. Attempting retry.", file=sys.stderr)
            if consecutive_match_api_errors >= CONSECUTIVE_API_ERROR_LIMIT:
                print(
                    "Too many consecutive match API errors. Stopping match fetch.", file=sys.stderr)
                fetch_match_error = match_error
                break
            continue  # Retry the same page

        consecutive_match_api_errors = 0  # Reset error count on success

        if not current_batch:  # API returned empty list (no more matches)
            print("\nNo more matches found (or no new matches after the last known ID).")
            break  # Stop if API returns no more data

        # NEW: Capture the newest match ID from the first successful batch
        if first_match_id_in_run is None:
            first_match_id_in_run = current_batch[0].get(
                'id') if current_batch else None
            if first_match_id_in_run:
                print(
                    f"\nNewest match ID found in this run: {first_match_id_in_run}")

        matches_data_aggregated.extend(current_batch)

        # Get the ID of the last match in this batch for the next 'before' cursor (to get older matches in the current run)
        if current_batch and current_batch[-1].get('id'):
            last_match_id_for_pagination = current_batch[-1]['id']
        else:
            print(
                f"\nWarning: Last match in batch {pages_fetched} missing 'id'. Cannot paginate further.", file=sys.stderr)
            break  # Stop pagination if ID is missing

        # Stop if the API returned fewer matches than requested (indicates end of history)
        if len(current_batch) < MATCHES_PER_PAGE:
            print(
                f"\nReached end of available match history (received {len(current_batch)} matches in last batch).")
            break

    # --- End of pagination loop ---
    print(
        f"\nFetched a total of {len(matches_data_aggregated)} matches across {pages_fetched} page(s).")

    # --- Process the aggregated matches ---
    if matches_data_aggregated:
        print(
            f"Processing {len(matches_data_aggregated)} matches for Elo/Nickname updates and new users...")
        now_utc_iso_match_phase = datetime.datetime.now(
            datetime.timezone.utc).isoformat()
        match_counter = 0

        for match in matches_data_aggregated:
            match_counter += 1
            progress_percent = (
                match_counter / len(matches_data_aggregated)) * 100
            print(
                f"\rProcessing match {match_counter}/{len(matches_data_aggregated)} ({progress_percent:.1f}%). Total unique users: {len(user_data_map)}...", end='', file=sys.stderr)
            sys.stderr.flush()

            players = match.get('players', [])
            for player in players:
                player_uuid = player.get('uuid')
                player_elo = player.get('eloRate')
                player_nick = player.get('nickname')

                if player_uuid:  # Only proceed if a valid UUID is present
                    if player_uuid in user_data_map:
                        # --- Existing User Logic ---
                        user_row = user_data_map[player_uuid]
                        last_scraped_str = user_row.get('last_scraped_at', '')
                        processed_match_players += 1

                        if should_update_user(last_scraped_str, UPDATE_INTERVAL_MINUTES):
                            update_made_in_match = False
                            new_elo_str = '' if player_elo is None else str(
                                player_elo)  # Handle null eloRate
                            if user_row.get('eloRate') != new_elo_str:
                                user_row['eloRate'] = new_elo_str
                                update_made_in_match = True
                            if player_nick and user_row.get('nickname') != player_nick:
                                user_row['nickname'] = player_nick
                                update_made_in_match = True

                            if update_made_in_match:
                                user_row['status'] = "OK Updated (Match)"
                                update_count_match += 1
                            else:
                                user_row['status'] = "OK Scraped (Match)"

                            # Mark for full profile fetch
                            uuids_to_fetch_full_profile.add(player_uuid)
                            # Update timestamp
                            user_row['last_scraped_at'] = now_utc_iso_match_phase

                        else:
                            # User recently scraped, just mark as skipped if no active status
                            if "OK" not in user_row.get('status', ''):
                                user_row['status'] = "OK (Skipped - Recent)"
                            skipped_recent_count += 1
                    else:
                        # --- NEW User Logic ---
                        new_user_row = {
                            'uuid': player_uuid,
                            'nickname': player_nick,
                            'eloRate': '' if player_elo is None else str(player_elo),
                            'twitch_name': '',  # Initialize empty, will be filled in next phase
                            # Mark as newly added from match
                            'status': 'New (Match)',
                            'last_scraped_at': now_utc_iso_match_phase,
                        }
                        # Ensure all baseline/original_headers are present for the new row, even if empty
                        for header in original_headers:
                            new_user_row.setdefault(header, '')

                        # Add to our in-memory map
                        user_data_map[player_uuid] = new_user_row
                        # Mark for full profile fetch
                        uuids_to_fetch_full_profile.add(player_uuid)
                        processed_match_players += 1  # Count this new user as processed
                        new_users_added_count += 1  # Increment new user counter

        print(
            f"\nFinished processing matches. Identified {len(uuids_to_fetch_full_profile)} users for Twitch update.")

    elif fetch_match_error:
        print(
            f"\nWarning: Could not fetch recent matches due to error: {fetch_match_error}. Skipping match update phase.", file=sys.stderr)
    else:
        print(f"\nNo matches fetched or processed.")

    print("-" * 30)

    # --- 3. Fetch Full Profiles (Phase 2 - Twitch Update) ---
    if uuids_to_fetch_full_profile:
        print(
            f"Fetching full profiles for {len(uuids_to_fetch_full_profile)} users to update Twitch names...")
        processed_user_api_count = 0
        consecutive_user_api_errors = 0
        uuid_list_to_fetch = list(uuids_to_fetch_full_profile)

        for idx, uuid_to_fetch in enumerate(uuid_list_to_fetch):
            processed_user_api_count = idx + 1
            print(
                f"\rFetching Twitch for user {processed_user_api_count}/{len(uuid_list_to_fetch)} ({uuid_to_fetch})...", end='', file=sys.stderr)
            sys.stderr.flush()

            if uuid_to_fetch not in user_data_map:
                print(
                    f"\nWarning: UUID {uuid_to_fetch} marked for fetch but not found in map. Skipping.", file=sys.stderr)
                continue

            user_api_url = USER_API_URL_TEMPLATE.format(uuid_to_fetch)
            fetched_data, error_code = get_api_data(user_api_url)
            now_utc_iso_user_phase = datetime.datetime.now(
                datetime.timezone.utc).isoformat()
            user_row = user_data_map[uuid_to_fetch]

            if fetched_data:
                consecutive_user_api_errors = 0
                twitch_updated = False
                connections = fetched_data.get('connections', {})
                twitch_info = connections.get(
                    'twitch') if connections else None
                new_twitch_name = twitch_info.get(
                    'name', '') if twitch_info else ''

                if user_row.get('twitch_name', '') != new_twitch_name:
                    user_row['twitch_name'] = new_twitch_name
                    twitch_updated = True
                    update_count_twitch += 1

                # Update status based on what happened
                current_status = user_row.get('status', '')
                if "New" in current_status:
                    user_row['status'] = "New (Match + Twitch)" if twitch_updated else "New (Match)"
                elif "Updated" in current_status:
                    user_row['status'] += " + Twitch" if twitch_updated else ""
                elif "Scraped" in current_status:
                    user_row['status'] = "OK Scraped (Match + Twitch)" if twitch_updated else "OK Scraped (Match)"
                else:
                    user_row['status'] = "OK Updated (Twitch)" if twitch_updated else "OK Scraped (Twitch)"

                user_row['last_scraped_at'] = now_utc_iso_user_phase
            else:
                print(
                    f"\nError fetching full profile for {uuid_to_fetch}: {error_code}", file=sys.stderr)
                if "Err" not in user_row.get('status', ''):
                    user_row['status'] += f" / Err Twitch ({error_code})"
                consecutive_user_api_errors += 1
                if consecutive_user_api_errors >= CONSECUTIVE_API_ERROR_LIMIT:
                    print(
                        "\nStopping Twitch fetch phase for this cycle due to consecutive errors.", file=sys.stderr)
                    break

            time.sleep(DELAY_USER_SECONDS)

        print(f"\nFinished fetching Twitch names.")
    else:
        print("No users identified as needing a Twitch name update in this cycle.")

    print("-" * 30)

    # --- 4. Write Updated Data Back ---
    print(f"Cycle Summary:")
    print(f"  New users added: {new_users_added_count}")
    print(f"  Elo/Nick updates from matches: {update_count_match}")
    print(f"  Twitch name updates: {update_count_twitch}")
    print(
        f"  Total updates skipped due to recent scrape: {skipped_recent_count}")
    print(f"  Total unique users in CSV after run: {len(user_data_map)}")

    final_data_list = list(user_data_map.values())

    if final_data_list:
        print(
            f"Saving {len(final_data_list)} updated user records back to {DATA_CSV_PATH}...")
        try:
            with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(
                    outfile, fieldnames=original_headers, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(final_data_list)
            print("Successfully saved updated data.")
        except IOError as e:
            print(
                f"\nError writing updated data to {DATA_CSV_PATH}: {e}. Changes lost for this run.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(
                f"\nAn unexpected error occurred during file writing: {e}. Changes lost.", file=sys.stderr)
            traceback.print_exc()
            sys.exit(1)
    else:
        print("No final data list generated to save.")

    # --- 5. Save the newest match ID for the next run ---
    if first_match_id_in_run is not None:
        write_last_match_id(LAST_MATCH_ID_FILE, first_match_id_in_run)
        print(
            f"Saved newest match ID ({first_match_id_in_run}) to {LAST_MATCH_ID_FILE} for next run.")
    else:
        print("No new matches found in this run to update the 'last_match_id.txt' file.")

except KeyboardInterrupt:
    print("\n--- Process interrupted by user. Saving current progress... ---", file=sys.stderr)
    # Perform a quick save if interrupted
    final_data_list = list(user_data_map.values())
    if final_data_list:
        try:
            with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(
                    outfile, fieldnames=original_headers, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(final_data_list)
            print("Successfully saved current data after interruption.",
                  file=sys.stderr)
        except IOError as e:
            print(f"Error saving data on interruption: {e}", file=sys.stderr)
    if first_match_id_in_run is not None:
        write_last_match_id(LAST_MATCH_ID_FILE, first_match_id_in_run)
        print(
            f"Saved newest match ID ({first_match_id_in_run}) to {LAST_MATCH_ID_FILE} on interruption.", file=sys.stderr)

except Exception as e:
    print(
        f"\n--- An unexpected error occurred during the script execution: {e} ---", file=sys.stderr)
    traceback.print_exc()
    sys.exit(1)

print(
    f"\n--- Update Cycle Finished at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
