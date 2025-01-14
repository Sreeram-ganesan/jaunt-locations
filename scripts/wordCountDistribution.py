import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Connect to the DuckDB database
con = duckdb.connect('data/locations.db')

# Query the word count distribution
data = con.execute("""
    SELECT word_count, COUNT(*) AS frequency
    FROM locations
    GROUP BY word_count
    ORDER BY word_count;
""").fetchall()

# Split data into x and y for plotting
word_counts = [row[0] for row in data]
frequencies = [row[1] for row in data]

# Plot the histogram
plt.figure(figsize=(10, 6))
plt.bar(word_counts, frequencies, color='skyblue')
plt.xlabel('Word Count')
plt.ylabel('Frequency')
plt.title('Histogram of Word Count in Descriptions')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# Plot the box plot
plt.figure(figsize=(8, 6))
plt.boxplot(frequencies, vert=False, patch_artist=True, boxprops=dict(facecolor='skyblue'))
plt.xlabel('Word Count')
plt.title('Box Plot of Word Count in Descriptions')
plt.grid(axis='x', linestyle='--', alpha=0.7)
plt.show()

# Plot the density plot
sns.kdeplot(word_counts, shade=True, color='blue')
plt.title('Density Plot of Word Count in Descriptions')
plt.xlabel('Word Count')
plt.ylabel('Density')
plt.grid(alpha=0.4)
plt.show()

# Plot the cumulative frequency plot
cumulative_frequencies = np.cumsum(frequencies) / sum(frequencies)
plt.plot(word_counts, cumulative_frequencies, marker='o')
plt.xlabel('Word Count')
plt.ylabel('Cumulative Frequency')
plt.title('Cumulative Frequency Plot')
plt.grid(alpha=0.4)
plt.show()

# Plot the density plot as percentage
sns.kdeplot(word_counts, fill=True, color="blue", alpha=0.5, common_norm=False)
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x * 100:.0f}%'))

# Labels and title
plt.xlabel("Word Count")
plt.ylabel("Percentage")
plt.title("Density Plot of Word Count as Percentage")
plt.grid(alpha=0.3)

# Display plot
plt.show()

