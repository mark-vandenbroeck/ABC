CREATE TABLE tunebooks 
( 
    id              SERIAL PRIMARY KEY, 
    url             TEXT UNIQUE NOT NULL, 
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
    status          TEXT DEFAULT '', 
    dispatched_at   TIMESTAMP WITH TIME ZONE
);