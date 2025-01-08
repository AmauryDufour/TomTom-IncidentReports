import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from utils.incidents_database import TrafficIncidentsDB
import os
import json
import logging
from datetime import datetime, timezone

dir_path = r"C:\Users\adufour\OneDrive - SystraGroup\Documents\TomTom\TomTom-IncidentReports\Singapore_TrafficIncidents"
db_path = r"C:\Users\adufour\OneDrive - SystraGroup\Documents\TomTom\TomTom-IncidentReports\Singapore_TrafficIncidents\Singapore_Incidents.db"
location = "Singapore" 
start_time = datetime.fromisoformat('2024-12-18T20:31:55+07:00')
end_time = datetime.fromisoformat('2024-12-27T18:03:18+07:00')

db = TrafficIncidentsDB(dir_path=dir_path, db_path=db_path, location=location)
db.optimize()

start_iso = start_time.isoformat()
end_iso = end_time.isoformat()

# Define SQL query
query = '''
    SELECT 
        id,
        startTime, 
        endTime, 
        category, 
        delay
    FROM incidents
    WHERE 
        (startTime BETWEEN ? AND ?) OR 
        (endTime BETWEEN ? AND ?)
'''

params = (start_iso, end_iso, start_iso, end_iso)

# Execute query
data = db.conn.execute(query, params).fetchall()

# Create DataFrame
columns = ['id', 'startTime', 'endTime', 'category', 'delay']
df = pd.DataFrame(data, columns=columns)

df['startTime'] = pd.to_datetime(df['startTime'], format='mixed')
df['endTime'] = pd.to_datetime(df['endTime'], format='mixed')

current_time = datetime.now(timezone.utc)
df.fillna({'endTime': current_time}, inplace=True)


# Define the mapping
icon_map_dict = {
    'Environmental Causes': [2, 3, 4, 5, 10, 11],
    'Human Car Breakdowns': [1, 14],
    'Jams': [6],
    'Planned Works Closures': [7, 8, 9],
    'Unknown Causes': [x for x in list(range(0, 100)) if x not in list(range(1, 12))+[14]]  # Default for any other category
}

def map_icon_category(category):
    for cause, categories in icon_map_dict.items():
        if int(category) in categories:
            return cause
    return 'Unknown Causes'  # Fallback

# Apply the mapping to create a new 'Cause' column
df['Cause'] = df['category'].map(map_icon_category)

# Calculate the duration in minutes for each incident
df['duration'] = (df['endTime'] - df['startTime']).dt.total_seconds() / 60

# Create a time interval column (e.g., hourly)
df['Interval'] = df['startTime'].dt.floor('h')

# Aggregate data per interval and cause
causes = list(icon_map_dict.keys())

# Pivot the data to get counts per cause per interval
cause_counts = df.pivot_table(index='Interval', columns='Cause', values='id', aggfunc='count', fill_value=0)

for cause in causes:
    if cause not in cause_counts.columns:
        cause_counts[cause] = 0

cause_counts['Total Causes'] = cause_counts.sum(axis=1)

for cause in causes:
    cause_counts[f'{cause} Share'] = (cause_counts[cause] / cause_counts['Total Causes']) * 100

delay_metrics = df.groupby('Interval').agg(
    Incidents_with_Delay=('delay', 'count'),
    Total_Delay=('delay', 'sum'),
    Average_Delay=('delay', 'mean')
).reset_index()

merged_df = cause_counts[[f'{cause} Share' for cause in causes]].merge(
    delay_metrics.set_index('Interval'),
    left_index=True,
    right_index=True
)

merged_df['Total_Incidents'] = cause_counts[causes].sum(axis=1)

sns.set_theme(style='whitegrid')
share_columns = [f'{cause} Share' for cause in causes]
fig, ax1 = plt.subplots(figsize=(14, 8))

# Plot the share of causes on the primary y-axis
merged_df[share_columns].plot(kind='area', stacked=True, ax=ax1, cmap='Pastel1')
ax1.set_xlabel('Timestamp', fontsize=14)
ax1.set_ylabel('Share of Causes (%)', fontsize=14)
ax1.set_xlim([start_time, end_time])
ax1.set_ylim(0, 100)

# Create secondary y-axis for Total Incidents
ax2 = ax1.twinx()
ax2.plot(merged_df.index, merged_df['Total_Incidents'], color='red', label='Total Incidents', linewidth=2)
ax2.set_ylabel('Total Incidents', fontsize=14, color='red')
ax2.tick_params(axis='y', labelcolor='red')

ax1.grid(True, which='both', axis='both', linewidth=0.5, linestyle='--', color='k')
ax1.set_axisbelow(False)

# Synchronize tick marks between ax1 and ax2
primary_ticks = ax1.get_yticks()
ymin1, ymax1 = ax1.get_ylim()  # (0, 100)
ymin2, ymax2 = ax2.get_ylim()  # (0, max_incidents)

# Calculate scaling factor and corresponding secondary ticks
scale_factor = (ymax2 - ymin2) / (ymax1 - ymin1) #  max_incidents / 100
secondary_ticks = ymin2 + (primary_ticks - ymin1) * scale_factor

# Ensure secondary ticks are rounded for better readability
secondary_ticks = [round(tick) for tick in secondary_ticks]

# Set the calculated ticks on secondary y-axis
ax2.set_yticks(secondary_ticks)
ax2.grid(False)


# Combine legends from both axes
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_2, [label.strip('Share').strip() for label in labels_1] + labels_2, loc='upper left', fontsize=12)

plt.title('Share of Incident Causes Over Time with Total Incidents', fontsize=16)
plt.tight_layout()
plt.savefig(os.path.join(dir_path, r"Share_of_Causes_Over_Time.png"))

# Create a figure with two subplots (stacked vertically)
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 14), sharex=True)

# ---------------------------
# Subplot 1: Incidents with Delay
# ---------------------------
sns.lineplot(
    data=merged_df,
    x=merged_df.index,
    y='Incidents_with_Delay',
    ax=ax1,
    color='blue',
    label='Incidents with Delay',
    linewidth=2,
    legend=False
)
ax1.set_ylabel('Number of Incidents', fontsize=14, color='blue')
ax1.tick_params(axis='y', labelcolor='blue')
ax1.set_xlim([start_time, end_time])

ax1_1 = ax1.twinx()
sns.lineplot(
    data=merged_df,
    x=merged_df.index,
    y='Average_Delay',
    ax=ax1_1,
    color='red',
    label='Average Delay (mins)',
    linewidth=2,
    legend=False
)
ax1_1.set_ylabel('Average Delay (mins)', fontsize=14, color='red')
ax1_1.tick_params(axis='y', labelcolor='red')
ax1_1.set_xlim([start_time, end_time])

# Synchronize tick marks between ax1 and ax2
primary_ticks = ax1.get_yticks()
ymin1, ymax1 = ax1.get_ylim()
ymin1_1, ymax1_1 = ax1_1.get_ylim()

# Calculate scaling factor and corresponding secondary ticks
scale_factor = (ymax1_1 - ymin1_1) / (ymax1 - ymin1)
secondary_ticks = ymin1_1 + (primary_ticks - ymin1) * scale_factor

# Ensure secondary ticks are rounded for better readability
secondary_ticks = [round(tick) for tick in secondary_ticks]

# Set the calculated ticks on secondary y-axis
ax1_1.set_yticks(secondary_ticks)
ax1_1.grid(False)

# Enable grid and position it above plot elements
ax1.grid(True, which='both', axis='both', linewidth=0.5, linestyle='--', color='k')
ax1.set_axisbelow(False)  # Moves grid lines to the foreground

# ---------------------------
# Subplot 2: Total Delay
# ---------------------------
sns.lineplot(
    data=merged_df,
    x=merged_df.index,
    y='Total_Delay',
    ax=ax2,
    color='green',
    label='Total Delay (mins)',
    linewidth=2,
    legend=False
)
ax2.set_ylabel('Delay (mins)', fontsize=14, color='green')
ax2.tick_params(axis='y', labelcolor='green')
ax2.set_xlim([start_time, end_time])

# Enable grid and position it above plot elements
ax2.grid(True, which='both', axis='both', linewidth=0.5, linestyle='--', color='b')
ax2.set_axisbelow(False)  # Moves grid lines to the foreground

plt.xlabel('Timestamp', fontsize=14)

# Combine legends from both subplots
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_1_1, labels_1_1 = ax1_1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
ax1.legend(lines_1 + lines_1_1 + lines_2, labels_1 + labels_1_1 + labels_2, loc='upper left', fontsize=12)

plt.title('Incidents and Delay Metrics Over Time', fontsize=16)
plt.tight_layout()

# Save the figure
plt.savefig(os.path.join(dir_path, r"Incidents_Delay_Metrics_Over_Time_Subplots.png"))

plt.show()
db.close()