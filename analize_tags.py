import pandas as pd
from collections import Counter

# carico il dataset
df = pd.read_csv('./hf_data/ranked_by_downloads_june.csv', sep=',')

df = df.dropna(subset=['tags'])  # rimuovo righe con 'tags' nulli
df = df[df['downloads'] > 0]  # filtro per downloads > 0

# prendo il 10%
df = df.iloc[:int(len(df) * 0.1)]

# conto la frequenza di ciascun tag
tag_counts = Counter(df['tags'])

# salvo i risultati su file
with open('file_txt/tag_counts.txt', 'w') as f:
    for tag, count in tag_counts.most_common():
        f.write(f"{tag}: {count}\n")