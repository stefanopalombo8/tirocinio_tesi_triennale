import pandas as pd
from collections import Counter

df = pd.read_csv('./hf_data/ranked_by_downloads_june.csv', sep=',')

df = df.dropna(subset=['tags'])  # rimuove righe con 'tags' nulli
df = df[df['downloads'] > 0]   

# prendo il 10%
df = df.iloc[:int(len(df) * 0.1)]

# conto la frequenza di ciascun tag
tag_counts = Counter(df['tags'])

# total_counted_tags = sum(tag_counts.values())
# print(total_counted_tags) # controllo se sono 20545

# salvo i risultati su file
with open('tag_counts.txt', 'w') as f:
    for tag, count in tag_counts.most_common():
        f.write(f"{tag}: {count}\n")