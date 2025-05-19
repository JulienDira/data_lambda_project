from whoosh.fields import Schema, TEXT, ID
from whoosh.index import create_in
import os
import pandas as pd

DATALAKE_ROOT = "C:/Users/julie/Documents/Streaming/data_lake"
INDEX_DIR = "index"

schema = Schema(path=ID(stored=True), content=TEXT(stored=True))
if not os.path.exists(INDEX_DIR):
    os.mkdir(INDEX_DIR)
ix = create_in(INDEX_DIR, schema)
writer = ix.writer()

for root, dirs, files in os.walk(DATALAKE_ROOT):
    for file in files:
        if file.endswith(".parquet"):
            full_path = os.path.join(root, file)
            df = pd.read_parquet(full_path)
            flat_text = df.astype(str).values.flatten()
            content = " ".join(flat_text)
            writer.add_document(path=full_path, content=content)
writer.commit()
print("Indexing completed.")
