DROP TABLE IF EXISTS urls CASCADE;

CREATE TABLE urls (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    downloaded_at TIMESTAMP WITH TIME ZONE,
    size_bytes INTEGER,
    status TEXT DEFAULT '',
    mime_type TEXT,
    document BYTEA,
    http_status INTEGER,
    retries INTEGER DEFAULT 0,
    dispatched_at TIMESTAMP WITH TIME ZONE,
    host TEXT,
    has_abc BOOLEAN,
    link_distance INTEGER DEFAULT 0,
    url_extension TEXT
);

CREATE INDEX idx_urls_host ON urls(host);
CREATE INDEX idx_urls_status ON urls(status);
CREATE INDEX idx_urls_status_created ON urls(status, created_at);
CREATE INDEX idx_urls_dispatched_at ON urls(dispatched_at);
CREATE INDEX idx_urls_retries ON urls(retries);
CREATE INDEX idx_urls_url_extension ON urls(url_extension);
CREATE INDEX idx_urls_purger_cleanup ON urls(status, has_abc); -- Removed document from index due to size limit
