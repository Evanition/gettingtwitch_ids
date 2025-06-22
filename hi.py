import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
from scipy.stats import norm  # Used for generating simulated data

# --- 1. Simulate Multi-Day Data (Replace with your actual data) ---
# Your ELO groups (x-axis labels)
elo_groups = np.arange(0, 2250, 50)  # From 0 to 2200, step 50
num_elo_groups = len(elo_groups)
num_days = 100  # Total number of days/frames for the animation

all_data_for_days = []
max_overall_height = 0  # To set a consistent y-axis limit

for day in range(num_days):
    # Simulate a normal distribution that subtly shifts and changes peak height
    # This will create the animated effect
    # Oscillate and add noise
    mean_elo = 950 + 50 * np.sin(day * 0.05) + np.random.normal(0, 50)
    # Oscillate and add noise
    std_dev_elo = 200 + 30 * np.cos(day * 0.03) + np.random.normal(0, 20)
    # Oscillate and add noise
    amplitude = 400 + 70 * np.sin(day * 0.07) + np.random.normal(0, 30)

    # Generate the 'number of people' for this day based on a normal distribution
    # We scale the PDF output to get realistic 'counts'
    daily_counts = amplitude * \
        norm.pdf(elo_groups, loc=mean_elo, scale=std_dev_elo)
    daily_counts = np.round(daily_counts).astype(int)  # Convert to integers
    daily_counts[daily_counts < 0] = 0  # Ensure no negative counts

    all_data_for_days.append(daily_counts)

    # Keep track of the maximum value to set a stable y-axis limit
    if daily_counts.max() > max_overall_height:
        max_overall_height = daily_counts.max()

print(f"Simulated data for {num_days} days generated.")
print(f"Max observed height across all days: {max_overall_height}")

# --- 2. Set up the Matplotlib Plot ---
fig, ax = plt.subplots(figsize=(12, 7))

# Initialize the bar chart with the data for the first day
# 'width' ensures bars are visible, 'align' centers them on the tick
bars = ax.bar(elo_groups, all_data_for_days[0], width=40,
              align='center', alpha=0.7, color='steelblue')

# Set static plot properties (labels, title, ticks, grid)
ax.set_xlabel("ELO Group (every 50)")
ax.set_ylabel("Number of People")
ax.set_title(f"Number of People by ELO Group (Day 1)")  # Initial title
ax.set_xticks(elo_groups[::2])  # Show every other ELO group tick for clarity
ax.set_xticklabels(elo_groups[::2], rotation=45, ha='right')
# Set y-axis limit slightly above max observed height
ax.set_ylim(0, max_overall_height * 1.15)
ax.grid(axis='y', linestyle='-', alpha=0.7)

# --- 3. Define the Animation Update Function ---


def update(frame):
    """
    This function is called for each frame of the animation.
    'frame' is the current frame number (0 to num_days - 1).
    """
    current_day_data = all_data_for_days[frame]

    # Update the height of each bar
    for i, bar in enumerate(bars):
        bar.set_height(current_day_data[i])

    # Update the title to show the current day
    ax.set_title(f"Number of People by ELO Group (Day {frame + 1})")

    # Return all artists that were modified
    return bars  # FuncAnimation expects an iterable of artists


# --- 4. Create the Animation ---
# interval: Delay between frames in milliseconds (100ms = 10 frames per second)
# blit: Set to True for optimized drawing (can sometimes cause issues, set to False if you see problems)
# init_func: Optional function to run once at the start to draw a clean slate for the animation.
#            It should return a tuple of artists that will be updated in each frame.
ani = animation.FuncAnimation(
    fig,
    update,
    frames=num_days,
    init_func=lambda: bars,  # Return the initial bars to be updated
    blit=True,
    interval=100
)

# --- 5. Show or Save the Animation ---
plt.tight_layout()  # Adjust plot to prevent labels from overlapping

# To display the animation in a pop-up window (comment out if saving only)
# plt.show()

# To save the animation as a GIF (requires Pillow: pip install Pillow)
# Or ImageMagick (often faster but needs external installation: imagemagick.org)
# print("Saving animation as GIF... This might take a few moments.")
# Use 'pillow' writer which is often easier to set up than 'imagemagick'
# ani.save('elo_group_distribution_animation.gif', writer='pillow', fps=10)
# print("Animation saved as elo_group_distribution_animation.gif")

# To save as an MP4 video (requires ffmpeg: ffmpeg.org)
print("Saving animation as MP4...")
ani.save('elo_group_distribution_animation.mp4', writer='ffmpeg', fps=10)
print("Animation saved as elo_group_distribution_animation.mp4")
