CREATE TABLE user_favorites (
    user_id TEXT NOT NULL,
    tune_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, tune_id),
    FOREIGN KEY (tune_id) REFERENCES tunes (id)
);
