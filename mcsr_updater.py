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
LAST_MATCH_ID_FILE = 'last_match_id.txt' # File to store the ID of the newest match seen

# API Request Delays
DELAY_MATCHES_SECONDS = 1.5              # Delay between match list API requests
DELAY_USER_SECONDS = 1.3                 # Delay between individual user API requests

# Retry Mechanism
MAX_RETRIES = 3                          # Max retries for API errors (like 429, timeouts)
RETRY_WAIT_SECONDS = 60                  # How long to wait after a 429 error
CONSECUTIVE_API_ERROR_LIMIT = 5          # Stop phase if this many consecutive API errors occur

# Fetching Logic
# Max number of *new* recent matches to attempt to fetch in THIS run.
# The script will stop if it fetches this many, OR if it hits matches already seen.
MAX_RECENT_MATCHES_TO_FETCH_PER_RUN = 1000
MATCHES_PER_PAGE = 100                   # Max allowed by API
MATCH_TYPE_FILTER = 2                    # NEW: Filter for Ranked Matches (2 = Ranked Match)

# Update Logic for Existing Users
UPDATE_INTERVAL_MINUTES = 10             # Don't update user's full profile if scraped within this interval
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
            headers = {'User-Agent': 'MCSRRankedDataUpdaterScript/1.5'} # Updated user agent version
            response = requests.get(
                url, params=params, headers=headers, timeout=25)

            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and data.get('status') == 'success':
                        return data.get('data'), None
                    elif isinstance(data, list):
                        return data, None
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
                return None, 404

            elif response.status_code == 429:
                print(
                    f"\nRate limit hit (429) for {url}. Waiting {RETRY_WAIT_SECONDS}s...", file=sys.stderr)
                time.sleep(RETRY_WAIT_SECONDS)
                retries += 1
                print(
                    f"Retrying {url} (Attempt {retries}/{MAX_RETRIES})...", file=sys.stderr)
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
                    pass
                return None, "HTTP 400"

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
                time.sleep(5)
                continue
            else:
                print(
                    f"\nMax retries exceeded for {url} after timeout.", file=sys.stderr)
                return None, "Timeout Error (Retries Exceeded)"

        except requests.exceptions.RequestException as e:
            print(f"\nNetwork/Request Error for {url}: {e}", file=sys.stderr)
            return None, "Network Error"

    print(f"\nMax retries exhausted for {url}. Giving up on this request.", file=sys.stderr)
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
            print(f"Warning: Could not read/parse {filepath}: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
    return None


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
user_data_map = {}
original_headers = ['uuid', 'nickname', 'eloRate', 'twitch_name', 'status', 'last_scraped_at']

update_count_match = 0
update_count_twitch = 0
skipped_recent_count = 0
new_users_added_count = 0
processed_match_players = 0
uuids_to_fetch_full_profile = set()

first_match_id_in_run = None
last_run_match_id = None

try:
    # --- 0. Read the last match ID from previous run ---
    last_run_match_id = read_last_match_id(LAST_MATCH_ID_FILE)
    print(f"Last match ID from previous run: {last_run_match_id if last_run_match_id else 'None (first run or file error)'}")

    # --- 1. Read existing user data OR create new CSV with headers ---
    if not os.path.exists(DATA_CSV_PATH):
        print(f"Data file '{DATA_CSV_PATH}' not found. Creating a new one with baseline headers.")
        try:
            with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=original_headers)
                writer.writeheader()
            print(f"Created new CSV: {DATA_CSV_PATH}")
        except IOError as e:
            print(f"Error creating new CSV file '{DATA_CSV_PATH}': {e}. Exiting.", file=sys.stderr)
            sys.exit(1)

    print(f"Reading existing data from {DATA_CSV_PATH}...")
    try:
        with open(DATA_CSV_PATH, 'r', newline='', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)

            if not reader.fieldnames:
                print(f"CSV file '{DATA_CSV_PATH}' has no content. Using baseline headers.")
            else:
                current_file_headers = list(reader.fieldnames)
                required_cols = ['uuid', 'eloRate', 'nickname']
                missing_req = [col for col in required_cols if col not in current_file_headers]
                if missing_req:
                    print(f"Error: CSV must contain required columns: {', '.join(missing_req)}. Exiting.", file=sys.stderr)
                    sys.exit(1)

                for col in ['status', 'last_scraped_at', 'twitch_name']:
                    if col not in current_file_headers:
                        current_file_headers.append(col)
                original_headers = current_file_headers

            all_rows_from_csv = list(reader)
            temp_map = {}
            rows_with_missing_uuid = 0
            for i, user_row in enumerate(all_rows_from_csv):
                uuid = user_row.get('uuid')
                if uuid:
                    for col in original_headers:
                        user_row.setdefault(col, '')
                    temp_map[uuid] = user_row
                else:
                    rows_with_missing_uuid += 1
                    for col in original_headers:
                        user_row.setdefault(col, '')
                    user_row['status'] = "Skipped (Missing UUID)"

            user_data_map = temp_map
            if rows_with_missing_uuid > 0:
                print(f"Warning: {rows_with_missing_uuid} row(s) in CSV have missing UUID and will be excluded from updates.", file=sys.stderr)

    except Exception as e:
        print(f"Error reading/initializing CSV: {e}. Exiting.", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    valid_users_in_map = len(user_data_map)
    print(f"Loaded {valid_users_in_map} users with valid UUIDs from CSV.")

    print(f"Attempting to fetch up to {MAX_RECENT_MATCHES_TO_FETCH_PER_RUN} NEW Ranked matches (Elo/Nick) and full profiles (Twitch).")
    print("-" * 30)

    # --- 2. Update/Add Users from Recent Matches (Phase 1 - PAGINATED) ---
    print(f"Fetching matches (in pages of {MATCHES_PER_PAGE})...")
    matches_data_aggregated = []
    last_match_id_for_pagination = None
    fetch_match_error = None
    pages_fetched = 0
    consecutive_match_api_errors = 0
    
    reached_old_matches = False 

    while len(matches_data_aggregated) < MAX_RECENT_MATCHES_TO_FETCH_PER_RUN and not reached_old_matches:
        pages_fetched += 1
        current_params = {'count': MATCHES_PER_PAGE, 'type': MATCH_TYPE_FILTER} # <--- ADDED MATCH_TYPE_FILTER HERE

        if pages_fetched == 1 and last_run_match_id:
            current_params['after'] = last_run_match_id
            print(f"\rFetching match page {pages_fetched} (after ID: {last_run_match_id}, type: {MATCH_TYPE_FILTER})...", end='', file=sys.stderr)
        elif last_match_id_for_pagination:
            current_params['before'] = last_match_id_for_pagination
            print(f"\rFetching match page {pages_fetched} (before ID: {last_match_id_for_pagination}, type: {MATCH_TYPE_FILTER})...", end='', file=sys.stderr)
        else:
            print(f"\rFetching match page {pages_fetched} (latest matches, type: {MATCH_TYPE_FILTER})...", end='', file=sys.stderr)

        sys.stderr.flush()

        time.sleep(DELAY_MATCHES_SECONDS)
        current_batch, match_error = get_api_data(
            MATCHES_API_URL, params=current_params)

        if match_error:
            consecutive_match_api_errors += 1
            print(f"\nError fetching match page {pages_fetched}: {match_error}. Attempting retry.", file=sys.stderr)
            if consecutive_match_api_errors >= CONSECUTIVE_API_ERROR_LIMIT:
                print("Too many consecutive match API errors. Stopping match fetch.", file=sys.stderr)
                fetch_match_error = match_error
                break
            continue

        consecutive_match_api_errors = 0

        if not current_batch:
            print("\nNo more matches found (or no new matches after the last known ID, or no matches of specified type).")
            break

        if first_match_id_in_run is None and current_batch:
            first_match_id_in_run = current_batch[0].get('id')
            if first_match_id_in_run:
                print(f"\nNewest match ID found in this run: {first_match_id_in_run}")

        matches_added_this_batch = 0

        for match in current_batch:
            match_id = match.get('id')
            if match_id is None:
                print(f"\nWarning: Match data missing 'id'. Skipping this match.", file=sys.stderr)
                continue

            if last_run_match_id is not None and match_id <= last_run_match_id:
                print(f"\nReached match ID {match_id} (<= last run's {last_run_match_id}). Stopping match fetching early.")
                reached_old_matches = True
                break
            
            if len(matches_data_aggregated) < MAX_RECENT_MATCHES_TO_FETCH_PER_RUN:
                matches_data_aggregated.append(match)
                matches_added_this_batch += 1
            else:
                print(f"\nReached target of {MAX_RECENT_MATCHES_TO_FETCH_PER_RUN} matches. Stopping early.")
                reached_old_matches = True
                break

        if reached_old_matches:
            break

        if matches_data_aggregated:
            last_match_id_for_pagination = matches_data_aggregated[-1].get('id')
        else:
            print("\nNo new matches were added from the last batch. Stopping match fetch.")
            break

        if len(current_batch) < MATCHES_PER_PAGE:
            print(
                f"\nReached end of available match history (received {len(current_batch)} matches in last batch).")
            break

    print(f"\nFetched a total of {len(matches_data_aggregated)} NEW matches across {pages_fetched} page(s).")

    # --- Process the aggregated matches ---
    if matches_data_aggregated:
        print(f"Processing {len(matches_data_aggregated)} matches for Elo/Nickname updates and new users...")
        now_utc_iso_match_phase = datetime.datetime.now(
            datetime.timezone.utc).isoformat()
        match_counter = 0

        for match in matches_data_aggregated:
            match_counter += 1
            progress_percent = (match_counter / len(matches_data_aggregated)) * 100
            print(f"\rProcessing match {match_counter}/{len(matches_data_aggregated)} ({progress_percent:.1f}%). Total unique users: {len(user_data_map)}...", end='', file=sys.stderr)
            sys.stderr.flush()

            players = match.get('players', [])
            for player in players:
                player_uuid = player.get('uuid')
                player_elo = player.get('eloRate')
                player_nick = player.get('nickname')

                if player_uuid:
                    if player_uuid in user_data_map:
                        user_row = user_data_map[player_uuid]
                        last_scraped_str = user_row.get('last_scraped_at', '')
                        processed_match_players += 1

                        if should_update_user(last_scraped_str, UPDATE_INTERVAL_MINUTES):
                            update_made_in_match = False
                            new_elo_str = '' if player_elo is None else str(player_elo)
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
                            
                            uuids_to_fetch_full_profile.add(player_uuid)
                            user_row['last_scraped_at'] = now_utc_iso_match_phase

                        else:
                            if "OK" not in user_row.get('status', ''):
                                user_row['status'] = "OK (Skipped - Recent)"
                            skipped_recent_count += 1
                    else:
                        new_user_row = {
                            'uuid': player_uuid,
                            'nickname': player_nick,
                            'eloRate': '' if player_elo is None else str(player_elo),
                            'twitch_name': '',
                            'status': 'New (Match)',
                            'last_scraped_at': now_utc_iso_match_phase,
                        }
                        for header in original_headers:
                            new_user_row.setdefault(header, '')

                        user_data_map[player_uuid] = new_user_row
                        uuids_to_fetch_full_profile.add(player_uuid)
                        processed_match_players += 1
                        new_users_added_count += 1

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
                print(f"\nWarning: UUID {uuid_to_fetch} marked for fetch but not found in map. Skipping.", file=sys.stderr)
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
                twitch_info = connections.get('twitch') if connections else None
                new_twitch_name = twitch_info.get('name', '') if twitch_info else ''

                if user_row.get('twitch_name', '') != new_twitch_name:
                    user_row['twitch_name'] = new_twitch_name
                    twitch_updated = True
                    update_count_twitch += 1

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
    print(f"  Total updates skipped due to recent scrape: {skipped_recent_count}")
    print(f"  Total unique users in CSV after run: {len(user_data_map)}")

    final_data_list = list(user_data_map.values())

    if final_data_list:
        print(f"Saving {len(final_data_list)} updated user records back to {DATA_CSV_PATH}...")
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
        print(f"Saved newest match ID ({first_match_id_in_run}) to {LAST_MATCH_ID_FILE} for next run.")
    else:
        print("No new matches found in this run to update the 'last_match_id.txt' file.")

except KeyboardInterrupt:
    print("\n--- Process interrupted by user. Saving current progress... ---", file=sys.stderr)
    final_data_list = list(user_data_map.values())
    if final_data_list:
        try:
            with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=original_headers, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(final_data_list)
            print("Successfully saved current data after interruption.", file=sys.stderr)
        except IOError as e:
            print(f"Error saving data on interruption: {e}", file=sys.stderr)
    if first_match_id_in_run is not None:
        write_last_match_id(LAST_MATCH_ID_FILE, first_match_id_in_run)
        print(f"Saved newest match ID ({first_match_id_in_run}) to {LAST_MATCH_ID_FILE} on interruption.", file=sys.stderr)

except Exception as e:
    print(
        f"\n--- An unexpected error occurred during the script execution: {e} ---", file=sys.stderr)
    traceback.print_exc()
    sys.exit(1)

print(
    f"\n--- Update Cycle Finished at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")