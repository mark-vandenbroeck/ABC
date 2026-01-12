-- Add FTS column
ALTER TABLE tunes ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Create GIN index
CREATE INDEX IF NOT EXISTS idx_tunes_search_vector ON tunes USING GIN(search_vector);

-- Create Trigger Function
CREATE OR REPLACE FUNCTION tunes_search_vector_update() RETURNS trigger AS $$
BEGIN
  NEW.search_vector :=
    setweight(to_tsvector('simple', coalesce(NEW.title, '')), 'A') ||
    setweight(to_tsvector('simple', coalesce(NEW.composer, '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(NEW."group", '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(NEW.rhythm, '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(NEW.book, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(NEW.area, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(NEW.origin, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(NEW.key, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(NEW.notes, '')), 'D') ||
    setweight(to_tsvector('simple', coalesce(NEW.transcription, '')), 'D') ||
    setweight(to_tsvector('simple', coalesce(NEW.history, '')), 'D') ||
    setweight(to_tsvector('simple', coalesce(NEW.source, '')), 'D');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create Trigger
DROP TRIGGER IF EXISTS tsvectorupdate ON tunes;
CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE
ON tunes FOR EACH ROW EXECUTE PROCEDURE tunes_search_vector_update();

-- Backfill exiting data
UPDATE tunes SET search_vector =
    setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
    setweight(to_tsvector('simple', coalesce(composer, '')), 'B') ||
    setweight(to_tsvector('simple', coalesce("group", '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(rhythm, '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(book, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(area, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(origin, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(key, '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(notes, '')), 'D') ||
    setweight(to_tsvector('simple', coalesce(transcription, '')), 'D') ||
    setweight(to_tsvector('simple', coalesce(history, '')), 'D') ||
    setweight(to_tsvector('simple', coalesce(source, '')), 'D');
