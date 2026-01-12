CREATE TABLE faiss_mapping (
    faiss_id INTEGER PRIMARY KEY,
    tune_id INTEGER NOT NULL,
    FOREIGN KEY (tune_id) REFERENCES tunes(id)
);
