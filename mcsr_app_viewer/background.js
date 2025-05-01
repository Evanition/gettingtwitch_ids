// --- Configuration ---
// !IMPORTANT: Replace with the ACTUAL RAW URL to your CSV file on GitHub
const GITHUB_CSV_URL = 'https://raw.githubusercontent.com/Evanition/gettingtwitch_ids/refs/heads/main/mcsr_user_data.csv'; // <-- MUST REPLACE
const STORAGE_KEY = 'mcsrUserDataMap'; // Key for chrome.storage.local

// --- State ---
let twitchUserDataMap = {}; // In-memory map: { twitchNameLower: { uuid, nickname, eloRate } }
let isDataLoaded = false;
let isFetching = false; // Prevent concurrent fetches

// --- Helper Functions ---

function parseCsvData(csvText) {
    // Same parsing function as before...
    console.log("MCSR Elo Viewer (Local): Parsing fetched CSV data...");
    const tempMap = {};
    try {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) {
            console.warn("MCSR Elo Viewer (Local): CSV appears empty or has no data rows.");
            return null;
        }
        const headers = lines[0].split(',').map(h => h.trim());
        const lowerCaseHeaders = headers.map(h => h.toLowerCase());
        const twitchNameIndex = lowerCaseHeaders.indexOf('twitch_name');
        const uuidIndex = lowerCaseHeaders.indexOf('uuid');
        const nicknameIndex = lowerCaseHeaders.indexOf('nickname');
        const eloRateIndex = lowerCaseHeaders.indexOf('elorate');

        if (twitchNameIndex === -1 || uuidIndex === -1 || nicknameIndex === -1 || eloRateIndex === -1) {
            console.error("MCSR Elo Viewer (Local): Fetched CSV missing required columns (uuid, twitch_name, nickname, eloRate). Headers found:", headers);
            return null;
        }

        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',').map(v => v.trim());
            if (values.length === headers.length) {
                const twitchName = values[twitchNameIndex];
                const uuid = values[uuidIndex];
                const nickname = values[nicknameIndex];
                const eloRateStr = values[eloRateIndex];

                if (twitchName && uuid) {
                    const lowerCaseTwitchName = twitchName.toLowerCase();
                    let eloRate = null;
                    if (eloRateStr) {
                        const parsedElo = parseInt(eloRateStr, 10);
                        if (!isNaN(parsedElo)) {
                            eloRate = parsedElo;
                        }
                    }
                    tempMap[lowerCaseTwitchName] = { uuid, nickname: nickname || null, eloRate };
                }
            } else if (lines[i].trim()) {
                 console.warn(`MCSR Elo Viewer (Local): Skipping malformed CSV row ${i + 1}.`);
            }
        }
        console.log(`MCSR Elo Viewer (Local): Successfully parsed ${Object.keys(tempMap).length} entries.`);
        return tempMap;
    } catch (error) {
        console.error("MCSR Elo Viewer (Local): Error parsing CSV data:", error);
        return null;
    }
}

// Renamed for clarity - this fetches AND updates storage
async function updateLocalDataFromGithub() {
    if (isFetching) {
        console.log("MCSR Elo Viewer (Local): Fetch already in progress, skipping manual update request.");
        return { success: false, message: "Fetch already in progress." };
    }
     if (GITHUB_CSV_URL === 'REPLACE_WITH_YOUR_RAW_GITHUB_CSV_URL') {
         console.error("MCSR Elo Viewer (Local): GITHUB_CSV_URL is not set!");
         return { success: false, message: "GitHub URL not configured." };
    }

    isFetching = true;
    console.log(`MCSR Elo Viewer (Local): Attempting to fetch data from GitHub for manual update...`);

    try {
        const response = await fetch(GITHUB_CSV_URL, { cache: 'no-store' });
        if (!response.ok) {
            throw new Error(`GitHub fetch failed! Status: ${response.status}`);
        }
        const csvText = await response.text();
        const parsedMap = parseCsvData(csvText);

        if (parsedMap && Object.keys(parsedMap).length > 0) {
            // --- Store fetched data locally ---
            await chrome.storage.local.set({ [STORAGE_KEY]: parsedMap });
            twitchUserDataMap = parsedMap; // Update in-memory map
            isDataLoaded = true; // Mark data as loaded (might have been false if storage was empty)
            console.log("MCSR Elo Viewer (Local): Successfully updated local data from GitHub.");
            return { success: true };
        } else {
            console.warn("MCSR Elo Viewer (Local): Parsing failed or resulted in empty map during update. Local data NOT overwritten.");
            return { success: false, message: "Failed to parse data from GitHub." };
        }

    } catch (error) {
        console.error("MCSR Elo Viewer (Local): Failed to fetch or store CSV data during update:", error);
         return { success: false, message: `Fetch error: ${error.message}` };
    } finally {
        isFetching = false;
    }
}

async function loadDataFromStorage() {
    console.log("MCSR Elo Viewer (Local): Attempting to load data from local storage...");
    try {
        const result = await chrome.storage.local.get(STORAGE_KEY);
        if (result[STORAGE_KEY] && Object.keys(result[STORAGE_KEY]).length > 0) {
            twitchUserDataMap = result[STORAGE_KEY];
            isDataLoaded = true;
            console.log(`MCSR Elo Viewer (Local): Loaded ${Object.keys(twitchUserDataMap).length} users from storage.`);
            return true; // Data loaded successfully
        } else {
            console.log("MCSR Elo Viewer (Local): No user data found in local storage.");
            isDataLoaded = false; // Ensure flag is false if storage is empty
            return false; // No data found
        }
    } catch (error) {
        console.error("MCSR Elo Viewer (Local): Error loading data from storage:", error);
        isDataLoaded = false;
        return false; // Error loading
    }
}

// --- Event Listeners ---

chrome.runtime.onInstalled.addListener(async (details) => {
    console.log("MCSR Elo Viewer (Local): onInstalled event triggered.", details.reason);
    const loaded = await loadDataFromStorage();
    // If it's the first install AND loading failed (storage empty), fetch initial data.
    if (details.reason === 'install' && !loaded) {
        console.log("MCSR Elo Viewer (Local): First install or empty storage, fetching initial data...");
        await updateLocalDataFromGithub(); // Fetch and store initial data
    }
});

chrome.runtime.onStartup.addListener(async () => {
    console.log("MCSR Elo Viewer (Local): onStartup event triggered.");
    await loadDataFromStorage(); // Load existing data when browser starts
});

// REMOVED: chrome.alarms.onAlarm listener is no longer needed

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "getElo") {
        // Ensure data is loaded before responding
        if (!isDataLoaded) {
            // Attempt to load from storage if memory is empty (e.g., service worker restart)
            loadDataFromStorage().then(() => {
                sendResponse(handleEloRequest(request.username));
            });
            return true; // Indicate async response
        } else {
            sendResponse(handleEloRequest(request.username));
        }
        return false; // Response sent synchronously (or handled by async above)

    } else if (request.action === "forceCsvUpdate") {
        console.log("MCSR Elo Viewer (Local): Manual update requested from popup.");
        // Call the update function and send response based on its result
        updateLocalDataFromGithub().then(result => {
            sendResponse(result); // Send {success: true/false, message?: string}
        });
        return true; // Indicate async response
    }
});

// Handles the lookup using the in-memory map
function handleEloRequest(username) {
    if (!isDataLoaded) {
        console.warn("MCSR Elo Viewer (Local): Data requested but not loaded for", username);
        return { elo: null, nickname: null, uuid: null, status: 'data_not_loaded' };
    }
    const lowerCaseUsername = username?.toLowerCase();
     if (!lowerCaseUsername) {
         return { elo: null, nickname: null, uuid: null, status: 'invalid_username' };
    }
    const userData = twitchUserDataMap[lowerCaseUsername];

    if (userData) {
        return { elo: userData.eloRate, nickname: userData.nickname, uuid: userData.uuid, status: 'local_storage_hit' };
    } else {
        return { elo: null, nickname: null, uuid: null, status: 'not_found_in_local_data' };
    }
}

// --- Initial Load Attempt ---
loadDataFromStorage(); // Attempt to load data when the script initializes

console.log("MCSR Elo Viewer (Local): Background script loaded.");