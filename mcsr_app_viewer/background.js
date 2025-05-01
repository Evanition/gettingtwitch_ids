// --- Configuration ---
// !IMPORTANT: Replace with the ACTUAL RAW URL to your CSV file on GitHub
// Example: const GITHUB_CSV_URL = 'https://raw.githubusercontent.com/YourUsername/YourRepo/main/mcsr_user_data.csv';
const GITHUB_CSV_URL = 'https://raw.githubusercontent.com/Evanition/gettingtwitch_ids/refs/heads/main/mcsr_user_data.csv'; // <-- MUST REPLACE

const UPDATE_ALARM_NAME = 'mcsrCsvUpdateAlarm';
const UPDATE_PERIOD_MINUTES = 60; // Check GitHub every 30 minutes (adjust as needed)
const STORAGE_KEY = 'mcsrUserDataMap'; // Key for chrome.storage.local

// --- State ---
let twitchUserDataMap = {}; // In-memory map: { twitchNameLower: { uuid, nickname, eloRate } }
let isDataLoaded = false;
let isFetching = false; // Prevent concurrent fetches

// --- Helper Functions ---

function parseCsvData(csvText) {
    console.log("MCSR Elo Viewer (GitHub): Parsing fetched CSV data...");
    const tempMap = {};
    try {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) {
            console.warn("MCSR Elo Viewer (GitHub): CSV appears empty or has no data rows.");
            return null;
        }
        const headers = lines[0].split(',').map(h => h.trim());
        // Find column indicesrobustly
        const lowerCaseHeaders = headers.map(h => h.toLowerCase());
        const twitchNameIndex = lowerCaseHeaders.indexOf('twitch_name');
        const uuidIndex = lowerCaseHeaders.indexOf('uuid');
        const nicknameIndex = lowerCaseHeaders.indexOf('nickname');
        const eloRateIndex = lowerCaseHeaders.indexOf('elorate'); // Check lowercase

        if (twitchNameIndex === -1 || uuidIndex === -1 || nicknameIndex === -1 || eloRateIndex === -1) {
            console.error("MCSR Elo Viewer (GitHub): Fetched CSV missing required columns (uuid, twitch_name, nickname, eloRate - case insensitive check). Headers found:", headers);
            return null;
        }

        for (let i = 1; i < lines.length; i++) {
            // Basic split, might need more robust parsing for quoted fields if your CSV uses them heavily
            const values = lines[i].split(',').map(v => v.trim());
            if (values.length === headers.length) {
                const twitchName = values[twitchNameIndex];
                const uuid = values[uuidIndex];
                const nickname = values[nicknameIndex];
                const eloRateStr = values[eloRateIndex];

                if (twitchName && uuid) { // Need at least twitch name and uuid
                    const lowerCaseTwitchName = twitchName.toLowerCase();
                    let eloRate = null;
                    if (eloRateStr) {
                        const parsedElo = parseInt(eloRateStr, 10);
                        if (!isNaN(parsedElo)) {
                            eloRate = parsedElo;
                        }
                    }
                    tempMap[lowerCaseTwitchName] = {
                        uuid,
                        nickname: nickname || null, // Use null if empty
                        eloRate // Can be null or number
                    };
                }
            } else if (lines[i].trim()) { // Log if row isn't empty but has wrong column count
                 console.warn(`MCSR Elo Viewer (GitHub): Skipping malformed CSV row ${i + 1}: ${lines[i]}`);
            }
        }
        console.log(`MCSR Elo Viewer (GitHub): Successfully parsed ${Object.keys(tempMap).length} entries.`);
        return tempMap;
    } catch (error) {
        console.error("MCSR Elo Viewer (GitHub): Error parsing CSV data:", error);
        return null;
    }
}

async function fetchAndStoreCsvData() {
    if (isFetching) {
        console.log("MCSR Elo Viewer (GitHub): Fetch already in progress, skipping.");
        return false; // Indicate fetch was skipped
    }
    if (GITHUB_CSV_URL === 'REPLACE_WITH_YOUR_RAW_GITHUB_CSV_URL') {
         console.error("MCSR Elo Viewer (GitHub): GITHUB_CSV_URL is not set in background.js!");
         return false;
    }

    isFetching = true;
    console.log(`MCSR Elo Viewer (GitHub): Attempting to fetch data from GitHub...`);

    try {
        const response = await fetch(GITHUB_CSV_URL, { cache: 'no-store' }); // Prevent browser caching
        if (!response.ok) {
            throw new Error(`GitHub fetch failed! Status: ${response.status}`);
        }
        const csvText = await response.text();
        const parsedMap = parseCsvData(csvText);

        if (parsedMap && Object.keys(parsedMap).length > 0) {
            await chrome.storage.local.set({ [STORAGE_KEY]: parsedMap });
            twitchUserDataMap = parsedMap; // Update in-memory map
            isDataLoaded = true;
            console.log("MCSR Elo Viewer (GitHub): Successfully updated data from GitHub.");
            return true; // Indicate success
        } else {
            console.warn("MCSR Elo Viewer (GitHub): Parsing failed or resulted in empty map. Keeping old data if available.");
            if (!isDataLoaded) await loadDataFromStorage(); // Ensure memory isn't empty if possible
            return false; // Indicate failure
        }

    } catch (error) {
        console.error("MCSR Elo Viewer (GitHub): Failed to fetch or store CSV data:", error);
        if (!isDataLoaded) await loadDataFromStorage(); // Try loading old data as fallback
        return false; // Indicate failure
    } finally {
        isFetching = false;
    }
}

async function loadDataFromStorage() {
    console.log("MCSR Elo Viewer (GitHub): Attempting to load data from local storage...");
    try {
        const result = await chrome.storage.local.get(STORAGE_KEY);
        if (result[STORAGE_KEY] && Object.keys(result[STORAGE_KEY]).length > 0) {
            twitchUserDataMap = result[STORAGE_KEY];
            isDataLoaded = true;
            console.log(`MCSR Elo Viewer (GitHub): Loaded ${Object.keys(twitchUserDataMap).length} users from storage.`);
        } else {
            console.log("MCSR Elo Viewer (GitHub): No user data found in local storage.");
            isDataLoaded = false;
        }
    } catch (error) {
        console.error("MCSR Elo Viewer (GitHub): Error loading data from storage:", error);
        isDataLoaded = false;
    }
}

function setupUpdateAlarm() {
    console.log(`MCSR Elo Viewer (GitHub): Setting up update alarm (${UPDATE_PERIOD_MINUTES} minutes).`);
    // Clear existing alarm first to ensure the period is updated if changed
    chrome.alarms.clear(UPDATE_ALARM_NAME, (wasCleared) => {
        chrome.alarms.create(UPDATE_ALARM_NAME, {
            delayInMinutes: 1, // Check shortly after startup/install/update
            periodInMinutes: UPDATE_PERIOD_MINUTES
        });
        console.log("MCSR Elo Viewer (GitHub): Update alarm created/reset.");
    });
}

// --- Event Listeners ---

chrome.runtime.onInstalled.addListener(async (details) => {
    console.log("MCSR Elo Viewer (GitHub): onInstalled event triggered.", details.reason);
    await loadDataFromStorage();
    if (!isDataLoaded || details.reason === 'install') { // Fetch on first install or if storage was empty
        console.log("MCSR Elo Viewer (GitHub): Fetching initial data from GitHub...");
        await fetchAndStoreCsvData();
    }
    setupUpdateAlarm();
});

chrome.runtime.onStartup.addListener(async () => {
    console.log("MCSR Elo Viewer (GitHub): onStartup event triggered.");
    await loadDataFromStorage();
    setupUpdateAlarm(); // Ensure alarm is set
});

chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === UPDATE_ALARM_NAME) {
        console.log("MCSR Elo Viewer (GitHub): Update alarm triggered, fetching data.");
        fetchAndStoreCsvData();
    }
});

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "getElo") {
        // If data isn't loaded yet (e.g., background script just started) try loading first
        if (!isDataLoaded) {
            loadDataFromStorage().then(() => {
                sendResponse(handleEloRequest(request.username));
            });
            return true; // Indicate async response needed
        } else {
            // Data is ready in memory
            sendResponse(handleEloRequest(request.username));
            return false; // Sync response sent
        }
    } else if (request.action === "forceCsvUpdate") {
        console.log("MCSR Elo Viewer (GitHub): Manual refresh requested.");
        if (isFetching) {
            sendResponse({ success: false, message: "Fetch already in progress." });
        } else {
            // Use async/await to wait for the fetch before responding success
            fetchAndStoreCsvData().then(success => {
                 sendResponse({ success: success });
            });
            return true; // Indicate async response
        }
    }
    // Default case or other messages
    // Return true if you might send an async response later for other message types
});

// Handles the lookup using the in-memory map
function handleEloRequest(username) {
    if (!isDataLoaded) {
        console.warn("MCSR Elo Viewer (GitHub): Data requested but not loaded for", username);
        return { elo: null, nickname: null, uuid: null, status: 'data_not_loaded' };
    }

    const lowerCaseUsername = username?.toLowerCase(); // Add safe navigation
    if (!lowerCaseUsername) {
         return { elo: null, nickname: null, uuid: null, status: 'invalid_username' };
    }

    const userData = twitchUserDataMap[lowerCaseUsername];

    if (userData) {
        return {
            elo: userData.eloRate,
            nickname: userData.nickname,
            uuid: userData.uuid,
            status: 'storage_hit'
        };
    } else {
        return { elo: null, nickname: null, uuid: null, status: 'not_found_in_map' };
    }
}

// --- Initial Load Attempt ---
loadDataFromStorage(); // Attempt to load data when the script initializes

console.log("MCSR Elo Viewer (GitHub): Background script loaded.");