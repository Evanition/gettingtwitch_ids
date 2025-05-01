const refreshButton = document.getElementById('refreshButton');
const statusDiv = document.getElementById('status');

refreshButton.addEventListener('click', () => {
  statusDiv.textContent = 'Requesting refresh...';
  refreshButton.disabled = true;
  // Send message to background to trigger fetch
  chrome.runtime.sendMessage({ action: "forceCsvUpdate" }, (response) => {
    if (chrome.runtime.lastError) {
      statusDiv.textContent = `Error: ${chrome.runtime.lastError.message}`;
    } else if (response && response.success) {
      statusDiv.textContent = 'Refresh triggered!';
      setTimeout(() => { statusDiv.textContent = ''; }, 3000); // Clear status after 3s
    } else {
       statusDiv.textContent = 'Refresh failed or already in progress.';
       setTimeout(() => { statusDiv.textContent = ''; }, 3000);
    }
    refreshButton.disabled = false;
  });
});