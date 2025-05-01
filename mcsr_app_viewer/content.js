console.log("MCSR Elo Viewer (Elo Only): Content script loaded.");

// --- Configuration ---
const ELO_DISPLAY_STYLE = 'badge'; // Keep as badge for visual display

// --- Rank Color Logic ---
/**
 * Determines rank information based on Elo.
 * @param {number|null} elo The player's Elo rating.
 * @returns {{rank: string, division: number|null, bgColor: string, textColor: string}} Rank info object.
 */
function getRankInfo(elo) {
    // Default for null Elo (unplaced) or errors
    const defaultRank = { rank: 'Unrated', division: null, bgColor: '#AAAAAA', textColor: '#000000' };

    if (elo === null || typeof elo !== 'number') {
        return defaultRank;
    }

    // Determine Rank and Division based on the image provided
    if (elo >= 2000) {
        return { rank: 'Netherite', division: null, bgColor: '#602A2A', textColor: '#FFFFFF' };
    } else if (elo >= 1800) {
        return { rank: 'Diamond', division: 3, bgColor: '#4AEED8', textColor: '#000000' };
    } else if (elo >= 1650) {
        return { rank: 'Diamond', division: 2, bgColor: '#4AEED8', textColor: '#000000' };
    } else if (elo >= 1500) {
        return { rank: 'Diamond', division: 1, bgColor: '#4AEED8', textColor: '#000000' };
    } else if (elo >= 1400) {
        return { rank: 'Emerald', division: 3, bgColor: '#00B837', textColor: '#FFFFFF' };
    } else if (elo >= 1300) {
        return { rank: 'Emerald', division: 2, bgColor: '#00B837', textColor: '#FFFFFF' };
    } else if (elo >= 1200) {
        return { rank: 'Emerald', division: 1, bgColor: '#00B837', textColor: '#FFFFFF' };
    } else if (elo >= 1100) {
        return { rank: 'Gold', division: 3, bgColor: '#F9A602', textColor: '#000000' };
    } else if (elo >= 1000) {
        return { rank: 'Gold', division: 2, bgColor: '#F9A602', textColor: '#000000' };
    } else if (elo >= 900) {
        return { rank: 'Gold', division: 1, bgColor: '#F9A602', textColor: '#000000' };
    } else if (elo >= 800) {
        return { rank: 'Iron', division: 3, bgColor: '#E0E0E0', textColor: '#000000' };
    } else if (elo >= 700) {
        return { rank: 'Iron', division: 2, bgColor: '#E0E0E0', textColor: '#000000' };
    } else if (elo >= 600) {
        return { rank: 'Iron', division: 1, bgColor: '#E0E0E0', textColor: '#000000' };
    } else if (elo >= 500) {
        return { rank: 'Coal', division: 3, bgColor: '#555555', textColor: '#FFFFFF' };
    } else if (elo >= 400) {
        return { rank: 'Coal', division: 2, bgColor: '#555555', textColor: '#FFFFFF' };
    } else { // 0-399
        return { rank: 'Coal', division: 1, bgColor: '#555555', textColor: '#FFFFFF' };
    }
}


// --- DOM Interaction ---
function addEloDisplay(usernameElement, eloData) {
    if (!usernameElement || usernameElement.classList.contains('mcsr-elo-processed')) {
        return;
    }
    usernameElement.classList.add('mcsr-elo-processed');

    const elo = eloData.elo;
    const nickname = eloData.nickname; // Still need nickname for the link title/functionality
    const { rank, division, bgColor, textColor } = getRankInfo(elo);

    let displayElo = elo !== null ? elo : 'N/A'; // Show N/A if Elo is null

    // Prevent adding multiple badges
    if (usernameElement.querySelector('.mcsr-elo-badge')) {
        return;
    }

    const badge = document.createElement('span');
    badge.classList.add('mcsr-elo-badge');

    // Basic Styling
    badge.style.marginLeft = '5px';
    badge.style.padding = '1px 5px';
    badge.style.borderRadius = '4px';
    badge.style.fontSize = '0.9em';
    badge.style.fontWeight = 'bold';
    badge.style.whiteSpace = 'nowrap';
    badge.style.fontFamily = 'monospace';

    // Apply Rank Colors
    badge.style.backgroundColor = bgColor;
    badge.style.color = textColor;

    // --- CHANGED: Only display Elo ---
    let badgeText = `${displayElo}`;
    badge.textContent = badgeText;
    // --- End Change ---

    // Add click functionality IF nickname exists (link still uses nickname)
    if (nickname) {
        badge.style.cursor = 'pointer';
        // --- CHANGED: Updated tooltip, removed explicit nickname mention ---
        badge.title = `Rank: ${rank}${division ? ' ' + ['?','I','II','III'][division] : ''} | Click to view ${nickname}'s Stats`;

        badge.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            const statsUrl = `https://mcsrranked.com/stats/${encodeURIComponent(nickname)}`;
            window.open(statsUrl, '_blank');
        });

         // Optional hover effect
         badge.addEventListener('mouseenter', () => {
             badge.style.filter = 'brightness(1.2)';
         });
         badge.addEventListener('mouseleave', () => {
             badge.style.filter = 'brightness(1)';
         });

    } else {
         // Tooltip when not clickable (nickname missing from data)
         // --- CHANGED: Updated tooltip ---
         badge.title = `Rank: ${rank}${division ? ' ' + ['?','I','II','III'][division] : ''} | Elo: ${displayElo}`;
    }

    // Insert badge
     if(usernameElement.parentNode) {
         usernameElement.parentNode.insertBefore(badge, usernameElement.nextSibling);
     } else {
         console.warn("MCSR Elo Viewer: Could not find parent node to insert badge for", usernameElement);
     }
}

// --- Message Handling & Username Detection (Keep the same) ---
function processNewChatMessages(mutations) {
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
                             chrome.runtime.sendMessage({ action: "getElo", username: username }, (response) => {
                                 const targetElement = usernameElement;
                                 if (chrome.runtime.lastError) {
                                     console.error("MCSR Elo Viewer (Elo Only):", chrome.runtime.lastError.message);
                                      targetElement.classList.remove('mcsr-elo-processing');
                                     return;
                                 }
                                 if (response && response.status !== 'not_found_in_map' && response.status !== 'data_not_loaded' && response.status !== 'invalid_username') {
                                     addEloDisplay(targetElement, response);
                                 } else {
                                     targetElement.classList.remove('mcsr-elo-processing');
                                 }
                                 if (!targetElement.classList.contains('mcsr-elo-processed')) {
                                      targetElement.classList.remove('mcsr-elo-processing');
                                 }
                             });
                        }
                     }
                });
             });
        });
    });
}


// --- Observer Setup (Keep the same) ---
const chatContainerSelector = '.chat-scrollable-area__message-container'; // Adjust if needed
let chatContainer = document.querySelector(chatContainerSelector);
const observer = new MutationObserver(processNewChatMessages);

function startObserver() {
    chatContainer = document.querySelector(chatContainerSelector);
    if (chatContainer) {
        console.log("MCSR Elo Viewer (Elo Only): Chat container found, starting observer.");
        observer.observe(chatContainer, { childList: true, subtree: true });
    } else {
        setTimeout(startObserver, 1500);
    }
}
startObserver();