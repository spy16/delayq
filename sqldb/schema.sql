CREATE TABLE IF NOT EXISTS __name__
(
    id      TEXT    NOT NULL PRIMARY KEY,
    value   TEXT    NOT NULL,
    version integer NOT NULL default 0,
    next_at integer NOT NULL
);

CREATE INDEX IF NOT EXISTS idx___name___next_at on __name__ (next_at);