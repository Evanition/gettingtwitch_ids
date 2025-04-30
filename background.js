// --- Configuration ---
// !IMPORTANT: Replace with the ACTUAL RAW URL to your CSV file on GitHub
const GITHUB_CSV_URL = 'https://raw.githubusercontent.com/Evanition/gettingtwitch_ids/refs/heads/main/mcsr_user_data.csv';
const UPDATE_ALARM_NAME = 'csvUpdateAlarm';
const UPDATE_PERIOD_MINUTES = 0.2;
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
            return null; // Not enough data
        }
        const headers = lines[0].split(',').map(h => h.trim());
        const twitchNameIndex = headers.indexOf('twitch_name');
        const uuidIndex = headers.indexOf('uuid');
        const nicknameIndex = headers.indexOf('nickname');
        const eloRateIndex = headers.indexOf('eloRate');

        if (twitchNameIndex === -1 || uuidIndex === -1 || nicknameIndex === -1 || eloRateIndex === -1) {
            console.error("MCSR Elo Viewer (GitHub): Fetched CSV missing required columns (uuid, twitch_name, nickname, eloRate).");
            return null; // Invalid format
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
            }
        }
        console.log(`MCSR Elo Viewer (GitHub): Successfully parsed ${Object.keys(tempMap).length} entries.`);
        return tempMap;
    } catch (error) {
        console.error("MCSR Elo Viewer (GitHub): Error parsing CSV data:", error);
        return null; // Parsing failed
    }
}

async function fetchAndStoreCsvData() {
    if (isFetching) {
        console.log("MCSR Elo Viewer (GitHub): Fetch already in progress, skipping.");
        return;
    }
    isFetching = true;
    console.log(`MCSR Elo Viewer (GitHub): Attempting to fetch data from ${GITHUB_CSV_URL}`);

    try {
        const response = await fetch(GITHUB_CSV_URL, { cache: 'no-store' }); // Prevent browser caching
        if (!response.ok) {
            throw new Error(`GitHub fetch failed! Status: ${response.status}`);
        }
        const csvText = await response.text();
        const parsedMap = parseCsvData(csvText);

        if (parsedMap && Object.keys(parsedMap).length > 0) {
            // Store the successfully parsed data
            await chrome.storage.local.set({ [STORAGE_KEY]: parsedMap });
            twitchUserDataMap = parsedMap; // Update in-memory map
            isDataLoaded = true;
            console.log("MCSR Elo Viewer (GitHub): Successfully updated data from GitHub and stored locally.");
        } else {
            console.warn("MCSR Elo Viewer (GitHub): Parsing failed or resulted in empty map. Keeping old data.");
            // Optionally load from storage again here if parsing failed, to ensure memory isn't empty
            if (!isDataLoaded) await loadDataFromStorage();
        }

    } catch (error) {
        console.error("MCSR Elo Viewer (GitHub): Failed to fetch or store CSV data:", error);
        // Keep using old data if fetch fails
        // Optionally load from storage again here as a fallback
         if (!isDataLoaded) await loadDataFromStorage();
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
            isDataLoaded = false; // Explicitly set if storage is empty/missing
        }
    } catch (error) {
        console.error("MCSR Elo Viewer (GitHub): Error loading data from storage:", error);
        isDataLoaded = false;
    }
}

function setupUpdateAlarm() {
    console.log(`MCSR Elo Viewer (GitHub): Setting up update alarm (${UPDATE_PERIOD_MINUTES} minutes).`);
    chrome.alarms.get(UPDATE_ALARM_NAME, (existingAlarm) => {
        // Only create if it doesn't exist, or if period changed (less critical here)
        if (!existingAlarm) {
            chrome.alarms.create(UPDATE_ALARM_NAME, {
                delayInMinutes: 5, // Initial delay before first check after setup
                periodInMinutes: UPDATE_PERIOD_MINUTES
            });
             console.log("MCSR Elo Viewer (GitHub): Update alarm created.");
        } else {
             console.log("MCSR Elo Viewer (GitHub): Update alarm already exists.");
        }
    });
}

// --- Event Listeners ---

// On Extension Install/Update
chrome.runtime.onInstalled.addListener(async (details) => {
    console.log("MCSR Elo Viewer (GitHub): onInstalled event triggered.", details.reason);
    await loadDataFromStorage(); // Try loading existing data first
    if (!isDataLoaded) {
        // If no data in storage (likely first install), fetch immediately
        console.log("MCSR Elo Viewer (GitHub): No data in storage on install, fetching from GitHub...");
        await fetchAndStoreCsvData();
    }
    setupUpdateAlarm(); // Setup the recurring alarm
});

// On Browser Startup
chrome.runtime.onStartup.addListener(async () => {
    console.log("MCSR Elo Viewer (GitHub): onStartup event triggered.");
    await loadDataFromStorage(); // Load data from storage
    setupUpdateAlarm(); // Ensure alarm is set (it might get cleared sometimes)
    // Optional: Trigger an immediate fetch on startup if desired, or let the alarm handle it
    // await fetchAndStoreCsvData();
});

// When the Alarm Fires
chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === UPDATE_ALARM_NAME) {
        console.log("MCSR Elo Viewer (GitHub): Update alarm triggered.");
        fetchAndStoreCsvData();
    }
});

// Message From Content Script
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "getElo") {
        // Ensure data is ready before responding
        if (!isDataLoaded) {
            // If data isn't loaded, try loading from storage first
            // This might happen if the background script was inactive
            loadDataFromStorage().then(() => {
                sendResponse(handleEloRequest(request.username));
            });
            return true; // Indicate async response
        } else {
            // Data is already in memory
             sendResponse(handleEloRequest(request.username));
        }
    }
     return true; // Keep channel open for async response in the if block
});

// Handles the actual lookup using the in-memory map
function handleEloRequest(username) {
     if (!isDataLoaded) {
        // This case should ideally be handled by the async logic in onMessage,
        // but as a fallback:
        console.warn("MCSR Elo Viewer (GitHub): Data requested but not loaded for", username);
        return { elo: null, nickname: null, uuid: null, status: 'data_not_loaded' };
    }

    const lowerCaseUsername = username.toLowerCase();
    const userData = twitchUserDataMap[lowerCaseUsername];

    if (userData) {
        return {
            elo: userData.eloRate,
            nickname: userData.nickname,
            uuid: userData.uuid,
            status: 'storage_hit' // Data came from storage (via memory)
        };
    } else {
        return { elo: null, nickname: null, uuid: null, status: 'not_found_in_map' };
    }
}

// --- Initial Load Attempt on Script Start ---
// Useful for when the service worker restarts
loadDataFromStorage();

console.log("MCSR Elo Viewer (GitHub): Background script loaded (auto-update version).");