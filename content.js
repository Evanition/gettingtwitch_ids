console.log("MCSR Elo Viewer (CSV): Content script loaded.");

// --- Configuration ---
const ELO_DISPLAY_STYLE = 'badge'; // 'badge' or 'tooltip' - Click only works well with 'badge'
const SHOW_MCSR_NICKNAME = true; // Keep true so the nickname is visible or used in title

// --- DOM Interaction ---
function addEloDisplay(usernameElement, eloData) {
    if (!usernameElement || usernameElement.classList.contains('mcsr-elo-processed')) {
        return;
    }
    usernameElement.classList.add('mcsr-elo-processed');

    const elo = eloData.elo;
    const nickname = eloData.nickname; // Get nickname from the data
    let displayElo = elo !== null ? elo : 'N/A';

    // Prevent adding multiple badges
    if (usernameElement.querySelector('.mcsr-elo-badge')) {
        return;
    }

    const badge = document.createElement('span');
    badge.classList.add('mcsr-elo-badge');

    // Basic Styling
    badge.style.marginLeft = '5px';
    badge.style.padding = '1px 4px';
    badge.style.borderRadius = '4px';
    badge.style.fontSize = '0.9em';
    badge.style.backgroundColor = '#6441A4';
    badge.style.color = 'white';
    badge.style.fontWeight = 'bold';
    badge.style.whiteSpace = 'nowrap';

    let badgeText = `${displayElo}`;
    if (SHOW_MCSR_NICKNAME && nickname) {
         badgeText += ` (${nickname})`;
    } else if (SHOW_MCSR_NICKNAME && !nickname) {
        // Optionally indicate if nickname is missing but was expected
        // badgeText += ` (?)`;
    }
    badge.textContent = badgeText;

    // --- NEW: Add click functionality IF nickname exists ---
    if (nickname) {
        badge.style.cursor = 'pointer'; // Make it look clickable
        badge.title = `Go to ${nickname}'s MCSR Stats`; // Tooltip for the badge itself

        badge.addEventListener('click', (event) => {
            event.preventDefault(); // Prevent any default span action
            event.stopPropagation(); // Stop the click from bubbling up

            // Construct the URL - Use encodeURIComponent for safety
            const statsUrl = `https://mcsrranked.com/stats/${encodeURIComponent(nickname)}`;
            window.open(statsUrl, '_blank'); // Open in a new tab
        });

         // Optional: Add hover effect
         badge.addEventListener('mouseenter', () => {
             badge.style.textDecoration = 'underline';
             badge.style.filter = 'brightness(1.2)';
         });
         badge.addEventListener('mouseleave', () => {
             badge.style.textDecoration = 'none';
             badge.style.filter = 'brightness(1)';
         });

    } else {
         badge.title = `MCSR Elo: ${displayElo} (Nickname not found in data)`;
    }
    // --- End of New Click Functionality ---


     if (ELO_DISPLAY_STYLE === 'tooltip') {
        // Note: Tooltip style doesn't lend itself well to clicking the ELO itself.
        // The click listener above is on the badge. If you force tooltip style,
        // the badge won't be visible, but the username's title will be set.
        // You *could* add the click listener to usernameElement instead if nickname exists.
        let titleText = `MCSR Elo: ${displayElo}`;
        if (SHOW_MCSR_NICKNAME && nickname) {
            titleText += ` (MCSR: ${nickname}) - Click badge to view stats`;
        } else {
            titleText += ` (Nickname not found in data)`;
        }
         usernameElement.title = titleText;
         // usernameElement.style.cursor = 'help'; // Maybe remove this if badge is primary interaction
    } else { // Default to badge
        usernameElement.parentNode.insertBefore(badge, usernameElement.nextSibling);
    }
}

// --- Message Handling & Username Detection (Keep the same as previous CSV version) ---
function processNewChatMessages(mutations) {
    mutations.forEach(mutation => {
        mutation.addedNodes.forEach(node => {
            // Check if the added node is a chat message container (adjust selector!)
            if (node.nodeType === Node.ELEMENT_NODE && node.matches('.chat-line__message, .chat-line__username-container, .text-fragment, .chat-author__display-name')) {
                 const usernameElements = node.querySelectorAll('[data-test-selector="chat-line-username"], .chat-author__display-name');

                usernameElements.forEach(usernameElement => {
                     if (usernameElement && !usernameElement.classList.contains('mcsr-elo-processed')) {
                        const username = usernameElement.textContent?.trim();
                        if (username) {
                            chrome.runtime.sendMessage({ action: "getElo", username: username }, (response) => {
                                if (chrome.runtime.lastError) {
                                    console.error("MCSR Elo Viewer (CSV):", chrome.runtime.lastError.message); return;
                                }
                                if (response && response.status !== 'not_found_in_map' && response.status !== 'data_loading') {
                                    // Re-finding element might still be needed if DOM shifts rapidly
                                     const currentElement = Array.from(document.querySelectorAll('.chat-author__display-name')).find(el => el.textContent.trim() === username && !el.classList.contains('mcsr-elo-processed'));
                                     const targetElement = currentElement || usernameElement;
                                     addEloDisplay(targetElement, response);
                                }
                            });
                        }
                     }
                });
            } else if (node.nodeType === Node.ELEMENT_NODE) {
                 // Sometimes messages are nested, recursively check children
                 const nestedUsernames = node.querySelectorAll('[data-test-selector="chat-line-username"], .chat-author__display-name');
                 nestedUsernames.forEach(usernameElement => {
                     if (usernameElement && !usernameElement.classList.contains('mcsr-elo-processed')) {
                         const username = usernameElement.textContent?.trim();
                         if(username) {
                             chrome.runtime.sendMessage({ action: "getElo", username: username }, (response) => {
                                 if (chrome.runtime.lastError) { console.error("MCSR Elo Viewer (CSV):", chrome.runtime.lastError.message); return; }
                                  if (response && response.status !== 'not_found_in_map' && response.status !== 'data_loading') {
                                      const currentElement = Array.from(document.querySelectorAll('.chat-author__display-name')).find(el => el.textContent.trim() === username && !el.classList.contains('mcsr-elo-processed'));
                                     const targetElement = currentElement || usernameElement;
                                     addEloDisplay(targetElement, response);
                                 }
                             });
                         }
                     }
                 });
            }
        });
    });
}


// --- Observer Setup (Keep the same as previous CSV version) ---
const chatContainerSelector = '.chat-scrollable-area__message-container';
let chatContainer = document.querySelector(chatContainerSelector);
const observer = new MutationObserver(processNewChatMessages);

function startObserver() {
    chatContainer = document.querySelector(chatContainerSelector);
    if (chatContainer) {
        console.log("MCSR Elo Viewer (CSV): Chat container found, starting observer.");
        observer.observe(chatContainer, { childList: true, subtree: true });
    } else {
        setTimeout(startObserver, 1000);
    }
}
startObserver();