console.log("MCSR Elo Viewer (SPA Ready): Content script loaded.");

// --- Configuration ---
const ELO_DISPLAY_STYLE = 'badge';
const chatContainerSelector = '.chat-scrollable-area__message-container'; // Adjust if Twitch changes this

// --- State Variables ---
let chatContainer = null; // Holds the currently observed chat container element
let chatObserver = null; // Holds the MutationObserver instance for messages
let observerReconnectTimer = null; // Timer for debouncing reconnect attempts

// --- Rank Color Logic (Keep the getRankInfo function as before) ---
function getRankInfo(elo) {
    // ... (keep the exact function from the previous version) ...
    const defaultRank = { rank: 'Unrated', division: null, bgColor: '#AAAAAA', textColor: '#000000' };
    if (elo === null || typeof elo !== 'number') return defaultRank;
    if (elo >= 2000) return { rank: 'Netherite', division: null, bgColor: '#602A2A', textColor: '#FFFFFF' };
    if (elo >= 1800) return { rank: 'Diamond', division: 3, bgColor: '#4AEED8', textColor: '#000000' };
    if (elo >= 1650) return { rank: 'Diamond', division: 2, bgColor: '#4AEED8', textColor: '#000000' };
    if (elo >= 1500) return { rank: 'Diamond', division: 1, bgColor: '#4AEED8', textColor: '#000000' };
    if (elo >= 1400) return { rank: 'Emerald', division: 3, bgColor: '#00B837', textColor: '#FFFFFF' };
    if (elo >= 1300) return { rank: 'Emerald', division: 2, bgColor: '#00B837', textColor: '#FFFFFF' };
    if (elo >= 1200) return { rank: 'Emerald', division: 1, bgColor: '#00B837', textColor: '#FFFFFF' };
    if (elo >= 1100) return { rank: 'Gold', division: 3, bgColor: '#F9A602', textColor: '#000000' };
    if (elo >= 1000) return { rank: 'Gold', division: 2, bgColor: '#F9A602', textColor: '#000000' };
    if (elo >= 900) return { rank: 'Gold', division: 1, bgColor: '#F9A602', textColor: '#000000' };
    if (elo >= 800) return { rank: 'Iron', division: 3, bgColor: '#E0E0E0', textColor: '#000000' };
    if (elo >= 700) return { rank: 'Iron', division: 2, bgColor: '#E0E0E0', textColor: '#000000' };
    if (elo >= 600) return { rank: 'Iron', division: 1, bgColor: '#E0E0E0', textColor: '#000000' };
    if (elo >= 500) return { rank: 'Coal', division: 3, bgColor: '#555555', textColor: '#FFFFFF' };
    if (elo >= 400) return { rank: 'Coal', division: 2, bgColor: '#555555', textColor: '#FFFFFF' };
    return { rank: 'Coal', division: 1, bgColor: '#555555', textColor: '#FFFFFF' };
}

// --- DOM Interaction (Keep addEloDisplay as before, hiding N/A) ---
function addEloDisplay(usernameElement, eloData) {
    const elo = eloData.elo;
    if (elo === null) return; // Don't display N/A
    if (!usernameElement || usernameElement.classList.contains('mcsr-elo-processed')) return;
    usernameElement.classList.add('mcsr-elo-processed');

    const nickname = eloData.nickname;
    const { rank, division, bgColor, textColor } = getRankInfo(elo);
    let displayElo = elo;

    if (usernameElement.querySelector('.mcsr-elo-badge')) return;

    const badge = document.createElement('span');
    // ... (rest of badge creation, styling, and click listener remains the same as the "Hide N/A" version) ...
     badge.classList.add('mcsr-elo-badge');
     badge.style.marginLeft = '5px';
     badge.style.padding = '1px 5px';
     badge.style.borderRadius = '4px';
     badge.style.fontSize = '0.9em';
     badge.style.fontWeight = 'bold';
     badge.style.whiteSpace = 'nowrap';
     badge.style.fontFamily = 'monospace';
     badge.style.backgroundColor = bgColor;
     badge.style.color = textColor;
     badge.textContent = `${displayElo}`;

     if (nickname) {
         badge.style.cursor = 'pointer';
         badge.title = `Rank: ${rank}${division ? ' ' + ['?','I','II','III'][division] : ''} | Click to view ${nickname}'s Stats`;
         badge.addEventListener('click', (event) => {
             event.preventDefault(); event.stopPropagation();
             window.open(`https://mcsrranked.com/stats/${encodeURIComponent(nickname)}`, '_blank');
         });
         badge.addEventListener('mouseenter', () => { badge.style.filter = 'brightness(1.2)'; });
         badge.addEventListener('mouseleave', () => { badge.style.filter = 'brightness(1)'; });
     } else {
         badge.title = `Rank: ${rank}${division ? ' ' + ['?','I','II','III'][division] : ''} | Elo: ${displayElo}`;
     }

     if(usernameElement.parentNode) {
         usernameElement.parentNode.insertBefore(badge, usernameElement.nextSibling);
     } else {
         console.warn("MCSR Elo Viewer: Could not find parent node to insert badge for", usernameElement);
     }
}

// --- Message Handling & Username Detection ---
function processNewChatMessages(mutations) {
    // Check if the container we are observing is still connected
    if (!chatContainer || !chatContainer.isConnected) {
        // ... (keep the reconnect logic from the previous version) ...
        console.log("MCSR Elo Viewer: Chat container disconnected, attempting to reconnect observer...");
        clearTimeout(observerReconnectTimer);
        observerReconnectTimer = setTimeout(initializeObserver, 1000);
        return;
    }

    mutations.forEach(mutation => {
        mutation.addedNodes.forEach(node => {
            const potentialMessageNodes = [node];
            if (node.nodeType === Node.ELEMENT_NODE) {
                 potentialMessageNodes.push(...node.querySelectorAll('.chat-line__message, .chat-line__username-container, .text-fragment'));
            }

            potentialMessageNodes.forEach(messageNode => {
                 if (messageNode.nodeType !== Node.ELEMENT_NODE) return;
                 const usernameElements = messageNode.querySelectorAll('[data-test-selector="chat-line-username"], .chat-author__display-name');

                 usernameElements.forEach(usernameElement => {
                     if (usernameElement && !usernameElement.classList.contains('mcsr-elo-processed') && !usernameElement.classList.contains('mcsr-elo-processing')) {
                        const username = usernameElement.textContent?.trim();
                        if (username) {
                             usernameElement.classList.add('mcsr-elo-processing');

                             // --- ADD THIS CHECK ---
                             // Check context validity *before* sending the message
                             if (!chrome.runtime?.id) {
                                 console.warn("MCSR Elo Viewer: Context invalidated before sending message for", username);
                                 // Clean up the processing flag as we are aborting
                                 usernameElement.classList.remove('mcsr-elo-processing');
                                 return; // Stop processing this specific username element
                             }
                             // --- END ADDED CHECK ---

                             // --- Wrap sendMessage in try...catch for extra safety ---
                             try {
                                 chrome.runtime.sendMessage({ action: "getElo", username: username }, (response) => {
                                     // Check context *again* inside the async callback
                                     if (!chrome.runtime?.id) {
                                          console.warn("MCSR Elo Viewer: Context invalidated before processing response for", username);
                                          // Attempt to clean up flag if element still exists somehow
                                          if(usernameElement) usernameElement.classList.remove('mcsr-elo-processing');
                                          return;
                                     }

                                     const targetElement = usernameElement; // Use original reference
                                     let processedSuccessfully = false;

                                     // Check lastError *first* in the callback
                                     if (chrome.runtime.lastError) {
                                         // Don't log the common "Receiving end does not exist" if background is just waking up
                                         if (chrome.runtime.lastError.message !== "Could not establish connection. Receiving end does not exist.") {
                                              console.error("MCSR Elo Viewer (Msg Handler):", chrome.runtime.lastError.message);
                                         }
                                     } else if (response && response.status !== 'not_found_in_local_data' && response.status !== 'data_not_loaded' && response.status !== 'invalid_username') {
                                         addEloDisplay(targetElement, response);
                                         if (targetElement.classList.contains('mcsr-elo-processed')) {
                                             processedSuccessfully = true;
                                         }
                                     }

                                     // Clean up processing flag if badge wasn't added
                                     if (!processedSuccessfully && targetElement) {
                                         targetElement.classList.remove('mcsr-elo-processing');
                                     }
                                 });
                             } catch (error) {
                                  console.error("MCSR Elo Viewer: Error sending message:", error);
                                  // Clean up flag if send message itself failed
                                  usernameElement.classList.remove('mcsr-elo-processing');
                             }
                             // --- End try...catch ---
                        }
                     }
                });
             });
        });
    });
}

// (Keep the rest of your content.js: initializeObserver, getRankInfo, addEloDisplay, observer setup etc.)
// --- Observer Setup ---
function initializeObserver() {
    // --- ADD THIS CHECK ---
    // Check if the extension context is still valid before doing anything
    // chrome.runtime?.id checks if chrome.runtime exists and then accesses id
    if (!chrome.runtime?.id) {
        console.log("MCSR Elo Viewer: Context invalidated, skipping observer initialization.");
        return; // Exit early, context is gone
    }
    // --- END ADDED CHECK ---


    console.log("MCSR Elo Viewer: Attempting to initialize observer...");
    // Disconnect previous observer if it exists
    if (chatObserver) {
        console.log("MCSR Elo Viewer: Disconnecting previous chat observer.");
        try { // Add try...catch around disconnect as it might also fail in invalid context
           chatObserver.disconnect();
        } catch (e) {
           console.warn("MCSR Elo Viewer: Error disconnecting old observer:", e.message);
        }
        chatObserver = null;
    }
    // Clear the reference to the old container
    chatContainer = null;

    // Try to find the new chat container
    chatContainer = document.querySelector(chatContainerSelector);

    if (chatContainer) {
        console.log("MCSR Elo Viewer: Chat container found, starting new observer.");
        // Create and start the observer for chat messages
        try { // Also wrap observer creation in try...catch for safety
            chatObserver = new MutationObserver(processNewChatMessages);
            chatObserver.observe(chatContainer, { childList: true, subtree: true });
        } catch (e) {
             console.error("MCSR Elo Viewer: Error creating or starting observer:", e);
             chatContainer = null; // Reset container if observing failed
        }

    } else {
        // If chat container not found, retry after a delay
        console.log("MCSR Elo Viewer: Chat container not found, retrying initialization in 2 seconds...");
        clearTimeout(observerReconnectTimer); // Clear any pending timer
        observerReconnectTimer = setTimeout(initializeObserver, 2000); // Wait 2 secs
    }
}

// --- Initial Kick-off ---
// The bodyObserver setup remains the same...
const bodyObserver = new MutationObserver((mutations) => {
    // ...(previous bodyObserver logic remains the same)...
     let chatMayHaveChanged = false;
     for (const mutation of mutations) {
         if (chatContainer && mutation.removedNodes) {
             for (const removedNode of mutation.removedNodes) {
                 if (removedNode === chatContainer || (removedNode.contains && removedNode.contains(chatContainer))) {
                      console.log("MCSR Elo Viewer: Detected removal of observed chat container.");
                      chatMayHaveChanged = true;
                      break;
                 }
             }
          }
          if (chatMayHaveChanged) break;
          if (mutation.addedNodes) {
               for (const addedNode of mutation.addedNodes) {
                   if (addedNode.nodeType === Node.ELEMENT_NODE) {
                        if (addedNode.matches && addedNode.matches(chatContainerSelector)) {
                             console.log("MCSR Elo Viewer: Detected addition of a new chat container element.");
                             chatMayHaveChanged = true;
                             break;
                        }
                        if (addedNode.querySelector && addedNode.querySelector(chatContainerSelector)) {
                             console.log("MCSR Elo Viewer: Detected addition of node containing a chat container.");
                             chatMayHaveChanged = true;
                             break;
                        }
                   }
               }
          }
           if (chatMayHaveChanged) break;
     }
     if (chatMayHaveChanged) {
          console.log("MCSR Elo Viewer: Re-initializing chat observer due to potential container change.");
          clearTimeout(observerReconnectTimer);
          observerReconnectTimer = setTimeout(initializeObserver, 500);
     }
});
// Ensure body exists before observing
if (document.body) {
    bodyObserver.observe(document.body, { childList: true, subtree: true });
} else {
    // If body isn't ready yet (less common with document_idle), wait
    window.addEventListener('DOMContentLoaded', () => {
         if (document.body) { // Double check body exists after DOMContentLoaded
              bodyObserver.observe(document.body, { childList: true, subtree: true });
         }
    });
}


// Initial attempt to set up the observer when the script first loads
initializeObserver();

// (Keep the rest of your content.js functions like getRankInfo, addEloDisplay, processNewChatMessages)