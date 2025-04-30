import requests
import time
import csv
import sys
import os
import datetime

# --- Configuration ---
USER_API_URL_TEMPLATE = "https://mcsrranked.com/api/users/{}"
MATCHES_API_URL = "https://mcsrranked.com/api/matches"
DATA_CSV_PATH = 'mcsr_user_data.csv'  # Input and Output file are the same now

DELAY_MATCHES_SECONDS = 2             # Short delay before/after matches request
# Delay between individual user API requests (for Twitch)
DELAY_USER_SECONDS = 1.3
MAX_RETRIES = 3                       # Max retries for errors (like 429)
RETRY_WAIT_SECONDS = 60               # How long to wait after a 429 error
RECENT_MATCH_COUNT = 100              # How many recent matches to fetch
# Don't update if scraped within this interval
UPDATE_INTERVAL_MINUTES = 10

# --- New Configuration for Continuous Run ---
# How often to run the entire update cycle (in minutes)
RUN_INTERVAL_MINUTES = 1
# Wait 5 minutes if CSV read/write fails before retrying cycle
FILE_ERROR_RETRY_DELAY_SECONDS = 300
# --- End Configuration ---


# --- Helper Functions (keep parse_timestamp, should_update_user, get_api_data as before) ---
def parse_timestamp(timestamp_str):
    """Safely parses an ISO timestamp string into a timezone-aware datetime object."""
    if not timestamp_str:
        return None
    try:
        # Handle potential 'Z' suffix for UTC explicitly if fromisoformat fails
        if isinstance(timestamp_str, str) and timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        dt = datetime.datetime.fromisoformat(timestamp_str)
        # Ensure timezone-aware, defaulting to UTC if naive
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            # print(f"\nWarning: Timestamp '{timestamp_str}' was naive, assuming UTC.", file=sys.stderr) # Optional warning
            return dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except ValueError:
        # print(f"\nWarning: Could not parse timestamp '{timestamp_str}'", file=sys.stderr) # Can be noisy
        return None
    except TypeError:
        # print(f"\nWarning: Invalid type for timestamp: {type(timestamp_str)}", file=sys.stderr)
        return None


def should_update_user(last_scraped_timestamp_str, update_interval_minutes):
    """Checks if a user should be updated based on the last scraped time."""
    last_scraped_dt = parse_timestamp(last_scraped_timestamp_str)
    if not last_scraped_dt:
        return True  # No valid timestamp, so update

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    time_since_last_update = now_utc - last_scraped_dt
    update_threshold = datetime.timedelta(minutes=update_interval_minutes)

    # Debug print (optional):
    # print(f"Now: {now_utc}, Last Scraped: {last_scraped_dt}, Diff: {time_since_last_update}, Threshold: {update_threshold}, Should Update: {time_since_last_update >= update_threshold}")

    return time_since_last_update >= update_threshold


def get_api_data(url, params=None):
    """Generic function to fetch data from API with retries."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            # Added user agent
            headers = {'User-Agent': 'MCSRRankedDataUpdaterScript/1.1'}
            response = requests.get(
                url, params=params, headers=headers, timeout=20)  # 20-second timeout

            if response.status_code == 200:
                try:
                    data = response.json()
                    # Handle successful dict response (like /users/{id})
                    if isinstance(data, dict) and data.get('status') == 'success':
                        return data.get('data'), None
                    # Handle successful list response (like /matches)
                    elif isinstance(data, list):
                        # Assuming list response is always success if status code is 200 for this API
                        return data, None
                    # Handle unexpected structure but 200 OK
                    else:
                        err_msg = f"Unexpected JSON structure or status field. Status: {data.get('status', 'N/A')}, Data Type: {type(data)}"
                        print(
                            f"\nAPI Logic Error at {url}. {err_msg}", file=sys.stderr)
                        return None, err_msg
                except ValueError:  # Includes JSONDecodeError
                    print(
                        f"\nInvalid JSON received from {url}. Status: {response.status_code}, Response Text: {response.text[:100]}...", file=sys.stderr)
                    return None, "Invalid JSON Response"

            elif response.status_code == 404:
                return None, 404  # Not found

            elif response.status_code == 429:
                print(
                    f"\nRate limit hit (429) for {url}. Waiting {RETRY_WAIT_SECONDS}s...", file=sys.stderr)
                time.sleep(RETRY_WAIT_SECONDS)
                retries += 1
                print(
                    f"Retrying {url} (Attempt {retries}/{MAX_RETRIES})...", file=sys.stderr)
                continue  # Retry the request

            # Specific check for 400 which might indicate no match data between users in versus endpoint etc.
            # Treat as 'no data' rather than a critical error for some endpoints, but log it.
            elif response.status_code == 400:
                print(
                    f"\nAPI returned 400 Bad Request for {url}. Params: {params}. Check if data exists or parameters are valid.", file=sys.stderr)
                # Try to get error message from response if possible
                try:
                    error_data = response.json()
                    if isinstance(error_data, dict) and error_data.get('status') == 'error':
                        print(
                            f"  API Error Message: {error_data.get('data')}", file=sys.stderr)
                        # Return specific API error
                        return None, f"API Error: {error_data.get('data')}"
                except ValueError:
                    pass  # Ignore if JSON decoding fails for error response
                return None, "HTTP 400"  # Generic 400 if no specific message

            else:
                # Other server-side or unexpected errors
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
            # Don't retry immediately for general network errors, could be persistent
            return None, "Network Error"

        # Should not be reached if logic is correct, but as a fallback:
        # Fallback if loop exited unexpectedly
        return None, f"HTTP {response.status_code}" if 'response' in locals() else "Request Failed"

    # If retries exhausted specifically for 429
    print(
        f"\nMax retries exceeded for {url} after rate limit errors.", file=sys.stderr)
    return None, "Rate Limit Retries Exhausted"


# --- Main Continuous Loop ---
while True:
    run_start_time = datetime.datetime.now()
    print(
        f"\n--- Starting Update Cycle at {run_start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    # --- Reset variables for this run ---
    all_user_data = []
    user_data_map = {}
    original_headers = []
    update_count_match = 0
    update_count_twitch = 0
    skipped_recent_count = 0
    processed_match_players = 0
    uuids_to_fetch_full_profile = set()
    file_operation_successful = True  # Flag to track if file ops worked

    try:
        # --- 1. Read existing data ---
        if not os.path.exists(DATA_CSV_PATH):
            print(
                f"Error: Data file '{DATA_CSV_PATH}' not found. Cannot update. Retrying after delay.", file=sys.stderr)
            file_operation_successful = False
            # Go directly to sleep at the end of the outer loop

        if file_operation_successful:  # Only proceed if file exists
            print(f"Reading existing data from {DATA_CSV_PATH}...")
            try:
                with open(DATA_CSV_PATH, 'r', newline='', encoding='utf-8') as infile:
                    reader = csv.DictReader(infile)
                    if not reader.fieldnames:
                        print(
                            f"Error: CSV file '{DATA_CSV_PATH}' appears empty or has no header. Retrying after delay.", file=sys.stderr)
                        file_operation_successful = False
                    else:
                        original_headers = list(reader.fieldnames)

                        # --- Column Validation ---
                        required_cols = ['uuid', 'eloRate', 'nickname']
                        missing_req = [
                            col for col in required_cols if col not in original_headers]
                        if missing_req:
                            print(
                                f"Error: CSV must contain required columns: {', '.join(missing_req)}. Retrying after delay.", file=sys.stderr)
                            file_operation_successful = False
                        else:
                            # --- Ensure optional columns exist ---
                            optional_cols = [
                                'status', 'last_scraped_at', 'twitch_name']
                            for col in optional_cols:
                                if col not in original_headers:
                                    print(f"Adding missing column: {col}")
                                    original_headers.append(col)

                            all_user_data = list(reader)

                            # --- Populate map and ensure defaults ---
                            temp_map = {}
                            rows_with_missing_uuid = 0
                            for i, user_row in enumerate(all_user_data):
                                uuid = user_row.get('uuid')
                                if uuid:
                                    # Ensure all expected columns exist in the row dict from CSV read
                                    for col in original_headers:
                                        # Use empty string as default for missing CSV values
                                        user_row.setdefault(col, '')
                                    temp_map[uuid] = user_row
                                else:
                                    rows_with_missing_uuid += 1
                                    # Mark directly in the list that will be written back
                                    user_row.setdefault(
                                        'status', 'Skipped (Missing UUID)')
                                    # Ensure other optional cols exist even for skipped rows
                                    user_row.setdefault('last_scraped_at', '')
                                    user_row.setdefault('twitch_name', '')
                            user_data_map = temp_map  # Assign after successful processing
                            if rows_with_missing_uuid > 0:
                                print(
                                    f"Warning: {rows_with_missing_uuid} row(s) in CSV have missing UUID.", file=sys.stderr)

            except IOError as e:
                print(
                    f"Error reading CSV file '{DATA_CSV_PATH}': {e}. Retrying after delay.", file=sys.stderr)
                file_operation_successful = False
            except Exception as e:
                print(
                    f"Unexpected error reading CSV: {e}. Retrying after delay.", file=sys.stderr)
                file_operation_successful = False

            # --- Check if we have valid data to process ---
            valid_users_in_map = len(user_data_map)
            if file_operation_successful and valid_users_in_map == 0 and rows_with_missing_uuid == len(all_user_data):
                print(
                    "CSV file contains no rows with valid UUIDs. Will only write back header/skipped rows.")
                # Allow proceeding to write phase, but skip API calls
                file_operation_successful = True  # Override to allow write back
                # Set flags to skip API calls
                matches_data = None
                uuids_to_fetch_full_profile.clear()

            elif file_operation_successful:
                total_users_in_csv = len(all_user_data)
                print(
                    f"Found {total_users_in_csv} total rows, {valid_users_in_map} users with UUIDs.")
                print(
                    f"Updating data based on last {RECENT_MATCH_COUNT} matches (Elo/Nick) and fetching Twitch names.")
                print("-" * 30)

        # --- Proceed only if file read was successful and there are users ---
        if file_operation_successful and valid_users_in_map > 0:

            # --- 2. Update ONLY from Recent Matches (Phase 1) ---
            print(f"Fetching last {RECENT_MATCH_COUNT} matches...")
            # Short delay before hitting matches API
            time.sleep(DELAY_MATCHES_SECONDS)
            matches_data, match_error = get_api_data(
                MATCHES_API_URL, params={'count': RECENT_MATCH_COUNT})

            if matches_data:
                print(
                    f"Processing {len(matches_data)} matches for Elo/Nickname updates...")
                now_utc_iso_match_phase = datetime.datetime.now(
                    datetime.timezone.utc).isoformat()
                match_counter = 0
                for match in matches_data:
                    match_counter += 1
                    print(
                        f"\rProcessing match {match_counter}/{len(matches_data)}...", end='', file=sys.stderr)
                    sys.stderr.flush()
                    # ... (rest of match processing logic as in the previous script) ...
                    players = match.get('players', [])
                    for player in players:
                        player_uuid = player.get('uuid')
                        player_elo = player.get('eloRate')  # Might be null
                        player_nick = player.get('nickname')

                        if player_uuid and player_uuid in user_data_map:
                            user_row = user_data_map[player_uuid]
                            last_scraped_str = user_row.get(
                                'last_scraped_at', '')
                            processed_match_players += 1  # Count potential updates considered

                            if should_update_user(last_scraped_str, UPDATE_INTERVAL_MINUTES):
                                update_made_in_match = False
                                # Update Elo if changed
                                new_elo_str = '' if player_elo is None else str(
                                    player_elo)
                                if user_row.get('eloRate') != new_elo_str:
                                    user_row['eloRate'] = new_elo_str
                                    update_made_in_match = True

                                # Update nickname if provided and different
                                if player_nick and user_row.get('nickname') != player_nick:
                                    user_row['nickname'] = player_nick
                                    update_made_in_match = True

                                # Set status based on match data update and add to fetch list
                                if update_made_in_match:
                                    user_row['status'] = "OK Updated (Match)"
                                    update_count_match += 1
                                else:
                                    # No data changed in match, but mark as scraped now
                                    user_row['status'] = "OK Scraped (Match)"

                                # Mark this user for a full profile fetch later for Twitch
                                uuids_to_fetch_full_profile.add(player_uuid)

                                # Update timestamp *temporarily* - will be overwritten if full fetch succeeds
                                user_row['last_scraped_at'] = now_utc_iso_match_phase
                            else:
                                # User was updated recently, skip all updates
                                # Avoid overwriting OK status from earlier match
                                if "OK" not in user_row.get('status', ''):
                                    user_row['status'] = "OK (Skipped - Recent)"
                                skipped_recent_count += 1

                print(
                    f"\nFinished processing matches. Identified {len(uuids_to_fetch_full_profile)} users for Twitch update.")

            elif match_error:
                print(
                    f"\nError fetching recent matches: {match_error}. Skipping match update phase for this cycle.", file=sys.stderr)
                # Continue processing, maybe Twitch updates are still possible if list populated from previous run? Unlikely.

            print("-" * 30)

            # --- 3. Fetch Full Profiles (Phase 2 - Twitch Update) ---
            if uuids_to_fetch_full_profile:
                print(
                    f"Fetching full profiles for {len(uuids_to_fetch_full_profile)} users to update Twitch names...")
                processed_user_api_count = 0
                consecutive_user_api_errors = 0
                # Convert set to list for ordered processing & progress indication
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
                    # Get the row reference
                    user_row = user_data_map[uuid_to_fetch]

                    if fetched_data:
                        consecutive_user_api_errors = 0
                        twitch_updated = False
                        # ... (rest of Twitch update logic as in the previous script) ...
                        connections = fetched_data.get('connections', {})
                        twitch_info = connections.get(
                            'twitch') if connections else None
                        new_twitch_name = twitch_info.get(
                            'name', '') if twitch_info else ''

                        # Only update if twitch name actually changed
                        if user_row.get('twitch_name', '') != new_twitch_name:
                            user_row['twitch_name'] = new_twitch_name
                            twitch_updated = True
                            update_count_twitch += 1

                        # Refine status - append Twitch info if updated
                        if twitch_updated:
                            if "Updated" in user_row['status']:
                                user_row['status'] += " + Twitch"
                            # If only scrape + twitch update
                            elif "Scraped" in user_row['status']:
                                user_row['status'] = "OK Scraped (Match) + Twitch"
                            else:
                                # Should be rare if match phase ran
                                user_row['status'] = "OK Updated (Twitch)"

                        # Update timestamp definitively after successful full fetch
                        user_row['last_scraped_at'] = now_utc_iso_user_phase

                    else:
                        print(
                            f"\nError fetching full profile for {uuid_to_fetch}: {error_code}", file=sys.stderr)
                        # Append error info
                        user_row['status'] += f" / Err Twitch ({error_code})"
                        consecutive_user_api_errors += 1

                        if consecutive_user_api_errors >= 5:
                            print(
                                "\nStopping Twitch fetch phase for this cycle due to consecutive errors.", file=sys.stderr)
                            break  # Stop trying to fetch more Twitch names

                    # Wait before next user API request
                    time.sleep(DELAY_USER_SECONDS)

                print(f"\nFinished fetching Twitch names.")
            else:
                print(
                    "No users identified as needing a Twitch name update in this cycle.")

            print("-" * 30)

        # --- 4. Write Updated Data Back ---
        # This happens even if API calls failed, to save status updates or column additions
        if file_operation_successful:  # Only write if read was okay initially
            print(f"Cycle Summary:")
            print(f"  Elo/Nick updates from matches: {update_count_match}")
            print(f"  Twitch name updates: {update_count_twitch}")
            print(
                f"  Total updates skipped due to recent scrape: {skipped_recent_count}")

            # Get the final data state from the map back into the list format
            final_data_list = []
            for row in all_user_data:  # Iterate original list to keep order & include skipped rows
                uuid = row.get('uuid')
                if uuid and uuid in user_data_map:
                    # Append the potentially modified row from map
                    final_data_list.append(user_data_map[uuid])
                elif 'Skipped (Missing UUID)' in row.get('status', ''):
                    # Append rows that were initially skipped
                    final_data_list.append(row)

            if final_data_list:
                print(f"Saving updated data back to {DATA_CSV_PATH}...")
                try:
                    with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                        # Use the potentially modified original_headers list
                        writer = csv.DictWriter(
                            outfile, fieldnames=original_headers, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(final_data_list)
                    print("Successfully saved updated data.")
                except IOError as e:
                    print(
                        f"\nError writing updated data to {DATA_CSV_PATH}: {e}. State may be lost for next cycle.", file=sys.stderr)
                    file_operation_successful = False  # Mark failure for potential longer delay
                except Exception as e:
                    print(
                        f"\nAn unexpected error occurred during file writing: {e}. State may be lost.", file=sys.stderr)
                    file_operation_successful = False
            else:
                print("No data available to save (check logs for errors).")

    except KeyboardInterrupt:
        print("\n--- Script interrupted by user. Exiting. ---")
        # Attempt to write one last time if possible (data might be partially processed)
        if 'final_data_list' in locals() and final_data_list:
            print(f"Attempting final save to {DATA_CSV_PATH}...")
            try:
                with open(DATA_CSV_PATH, 'w', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(
                        outfile, fieldnames=original_headers, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(final_data_list)
                print("Final save successful.")
            except Exception as e:
                print(f"Error during final save: {e}", file=sys.stderr)
        break  # Exit the while True loop

    except Exception as e:
        print(
            f"\n--- An unexpected error occurred during the main loop: {e} ---", file=sys.stderr)
        # Log traceback for debugging if needed
        import traceback
        traceback.print_exc()
        print("Continuing to next cycle after delay...")
        file_operation_successful = False  # Assume file state might be bad

    # --- Pause before next cycle ---
    if file_operation_successful:
        sleep_duration_seconds = RUN_INTERVAL_MINUTES * 60
        print(
            f"\n--- Cycle Complete. Waiting {RUN_INTERVAL_MINUTES} minutes until next run. ---")
    else:
        sleep_duration_seconds = FILE_ERROR_RETRY_DELAY_SECONDS
        print(
            f"\n--- Cycle completed with file errors. Waiting {FILE_ERROR_RETRY_DELAY_SECONDS / 60:.1f} minutes before retrying. ---")

    try:
        time.sleep(sleep_duration_seconds)
    except KeyboardInterrupt:
        print("\n--- Script interrupted during sleep. Exiting. ---")
        break # Exit the while True loop
