let twitchUserDataMap = {}; // Stores { twitchNameLowercase: { uuid, nickname, eloRate } }
let isLoadingData = false;
let isDataLoaded = false;

// --- Data Loading ---
async function loadAndProcessCSV() {
    if (isLoadingData || isDataLoaded) return;
    isLoadingData = true;
    console.log("MCSR Elo Viewer (CSV): Loading CSV data...");
    try {
        const response = await fetch(chrome.runtime.getURL('mcsr_user_data.csv'));
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const text = await response.text();
        const lines = text.trim().split('\n'); // Trim whitespace and split
        const headers = lines[0].split(',').map(h => h.trim());

        // Find column indices
        const twitchNameIndex = headers.indexOf('twitch_name');
        const uuidIndex = headers.indexOf('uuid');
        const nicknameIndex = headers.indexOf('nickname');
        const eloRateIndex = headers.indexOf('eloRate'); // Get Elo index

        if (twitchNameIndex === -1 || uuidIndex === -1 || nicknameIndex === -1 || eloRateIndex === -1) {
            console.error("MCSR Elo Viewer (CSV): CSV headers missing required columns (uuid, twitch_name, nickname, eloRate).");
            isLoadingData = false;
            return;
        }

        const tempMap = {};
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',').map(v => v.trim());
            if (values.length === headers.length) {
                const twitchName = values[twitchNameIndex];
                const uuid = values[uuidIndex];
                const nickname = values[nicknameIndex];
                const eloRateStr = values[eloRateIndex];

                if (twitchName && uuid) { // Only map if twitch_name and uuid exist
                    const lowerCaseTwitchName = twitchName.toLowerCase();
                    let eloRate = null;
                    if (eloRateStr) { // Check if eloRate string is not empty
                         const parsedElo = parseInt(eloRateStr, 10);
                         if (!isNaN(parsedElo)) { // Check if it's a valid number
                             eloRate = parsedElo;
                         }
                    }

                    tempMap[lowerCaseTwitchName] = {
                        uuid: uuid,
                        nickname: nickname || null, // Store nickname or null
                        eloRate: eloRate // Store parsed Elo or null
                    };
                }
            }
        }
        twitchUserDataMap = tempMap;
        isDataLoaded = true;
        console.log(`MCSR Elo Viewer (CSV): Loaded ${Object.keys(twitchUserDataMap).length} user data entries from CSV.`);

    } catch (error) {
        console.error("MCSR Elo Viewer (CSV): Failed to load or process CSV:", error);
        isDataLoaded = false;
    } finally {
        isLoadingData = false;
    }
}


// --- Message Handling ---
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "getElo") {
        // Ensure data is loaded before proceeding
        if (!isDataLoaded && !isLoadingData) {
            loadAndProcessCSV().then(() => { // Ensure data is loaded
                 sendResponse(handleEloRequest(request.username));
            });
        } else if (isLoadingData) {
             const checkInterval = setInterval(() => {
                 if (!isLoadingData) {
                     clearInterval(checkInterval);
                     sendResponse(handleEloRequest(request.username));
                 }
             }, 100);
        }
        else {
            sendResponse(handleEloRequest(request.username)); // Send response immediately if data loaded
        }
        return true; // Indicates response might be sent asynchronously
    }
});

// Synchronous function now, as data is in memory
function handleEloRequest(username) {
     if (!isDataLoaded) {
        console.warn("MCSR Elo Viewer (CSV): Data not loaded yet for", username);
        return { elo: null, nickname: null, uuid: null, status: 'data_loading' };
    }

    const lowerCaseUsername = username.toLowerCase();
    const userData = twitchUserDataMap[lowerCaseUsername];

    if (userData) {
        // console.log(`MCSR Elo Viewer (CSV): Found data for ${username}:`, userData);
        return {
            elo: userData.eloRate, // Use the eloRate from the map
            nickname: userData.nickname,
            uuid: userData.uuid,
            status: 'csv_hit'
        };
    } else {
        // console.log(`MCSR Elo Viewer (CSV): No data found for Twitch user: ${username}`);
        return { elo: null, nickname: null, uuid: null, status: 'not_found_in_map' };
    }
}


// --- Initial Load ---
loadAndProcessCSV();

// Re-load on startup/install just in case
chrome.runtime.onStartup.addListener(loadAndProcessCSV);
chrome.runtime.onInstalled.addListener(loadAndProcessCSV);

console.log("MCSR Elo Viewer (CSV): Background script loaded.");