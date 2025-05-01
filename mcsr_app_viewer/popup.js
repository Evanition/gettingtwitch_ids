const refreshButton = document.getElementById('refreshButton');
const statusDiv = document.getElementById('status');

refreshButton.addEventListener('click', () => {
  // More user-friendly status
  statusDiv.textContent = 'Checking for new data...';
  refreshButton.disabled = true;

  chrome.runtime.sendMessage({ action: "forceCsvUpdate" }, (response) => {
    if (chrome.runtime.lastError) {
      // Friendlier error for this specific case
      statusDiv.textContent = `Error: Extension component missing. Try reloading the extension.`;
      console.error("Popup Error:", chrome.runtime.lastError.message); // Keep technical log
    } else if (response && response.success) {
      statusDiv.textContent = 'Update check started!'; // Confirms action started
      setTimeout(() => { statusDiv.textContent = ''; }, 3500); // Clear status after 3.5s
    } else if (response && !response.success && response.message === "Fetch already in progress.") {
       statusDiv.textContent = 'Already checking for updates.';
       setTimeout(() => { statusDiv.textContent = ''; }, 3500);
    }
    else {
       statusDiv.textContent = 'Could not start update check. Please try again later.';
        setTimeout(() => { statusDiv.textContent = ''; }, 3500);
    }
    // Re-enable button slightly later to prevent rapid clicks if response is fast
    setTimeout(() => { refreshButton.disabled = false; }, 500);
  });
});