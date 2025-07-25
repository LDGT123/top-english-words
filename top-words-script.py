import requests
import json
from collections import Counter
import re
from tqdm import tqdm

# Tokenizer
def tokenize(text):
    return re.findall(r'\b[a-zA-Z]+\b', text.lower())

# Download the file
def download_jsonl(url, local_path):
    print("🌐 Downloading Books.jsonl...") # Books.jsonl is 20 GB. This might take a while, depending on your internet speed.
    response = requests.get(url, stream=True)
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"✅ Download complete: saved to {local_path}")

# Process the file and extract top words
def build_top_words_jsonl(jsonl_path, limit=200000, output_file="top_200k_books.txt"): # Change "limit" based on how many words you want
    counter = Counter()
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="🔍 Processing reviews", unit="review"):
            try:
                review = json.loads(line)
                text = review.get("text", "") or review.get("reviewText", "")
                counter.update(tokenize(text))
            except json.JSONDecodeError:
                continue

    top_words = [word for word, _ in counter.most_common(limit)]
    with open(output_file, "w", encoding="utf-8") as f:
        for word in top_words:
            f.write(f"{word}\n")

    print(f"\n✅ Done! Saved top {len(top_words)} words to '{output_file}'")

# Main runner
if __name__ == "__main__":
    url = "your-url-here" # See the README for instructions on how to get this link
    local_path = "Books.jsonl"

    download_jsonl(url, local_path)
    build_top_words_jsonl(local_path)
