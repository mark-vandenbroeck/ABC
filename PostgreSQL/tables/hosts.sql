CREATE TABLE hosts (
    host TEXT PRIMARY KEY,
    last_access TIMESTAMP WITH TIME ZONE,
    last_http_status INTEGER,
    downloads INTEGER DEFAULT 0,
    disabled BOOLEAN DEFAULT FALSE,
    disabled_reason TEXT,
    disabled_at TIMESTAMP WITH TIME ZONE
);
