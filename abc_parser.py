import sys
import json
import re
import logging
from datetime import datetime
from database import get_db_connection
try:
    from music21 import converter, note as m21_note
    MUSIC21_AVAILABLE = True
except ImportError:
    MUSIC21_AVAILABLE = False

logger = logging.getLogger(__name__)

class Tune:
    # Full mapping of ABC header keys to database column names
    METADATA_MAPPING = {
        'X': 'reference_number',
        'T': 'title',
        'C': 'composer',
        'O': 'origin',
        'A': 'area',
        'M': 'meter',
        'L': 'unit_note_length',
        'Q': 'tempo',
        'P': 'parts',
        'Z': 'transcription',
        'N': 'notes',
        'G': 'group',
        'H': 'history',
        'K': 'key',
        'R': 'rhythm',
        'B': 'book',
        'D': 'discography',
        'S': 'source',
        'I': 'instruction'
    }

    MAX_TUNE_CHARS = 10000 # Skip extremely large tunes (e.g. symphonies)
    MAX_TUNE_LINES = 300
    MAX_VOICES = 4 # Skip complex multi-voice orchestral scores

    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.metadata = {}
        self.elements = []
        self.title = "Untitled"
        self.tune_body = ""
        self.pitches = []
        self.status = "parsed"
        self.skip_reason = None
        
        if len(raw_data) > self.MAX_TUNE_CHARS:
            self.status = "skipped"
            self.skip_reason = "too_large"
            logger.warning(f"Skipping tune: raw data too large ({len(raw_data)} chars)")
            self._parse_headers_only()
            return

        # Check line count
        line_count = raw_data.count('\n') + 1
        if line_count > self.MAX_TUNE_LINES:
            self.status = "skipped"
            self.skip_reason = "too_many_lines"
            logger.warning(f"Skipping tune: too many lines ({line_count})")
            self._parse_headers_only()
            return
            
        # Quick complexity check: count voices
        voice_count = len(re.findall(r'^V:\s*', raw_data, re.MULTILINE))
        if voice_count > self.MAX_VOICES:
            self.status = "skipped"
            self.skip_reason = "too_many_voices"
            logger.warning(f"Skipping tune: too many voices ({voice_count})")
            self._parse_headers_only()
            return

        self._parse()

    def _parse_headers_only(self):
        """Minimal parsing to extract title and metadata when tune is skipped"""
        lines = self.raw_data.strip().split('\n')
        header_pattern = re.compile(r'^([A-Z]):\s*(.*)$')
        for line in lines:
            line_stripped = line.strip()
            match = header_pattern.match(line_stripped)
            if match:
                key, value = match.groups()
                value = re.sub(r'%.*', '', value).strip()
                if key in self.METADATA_MAPPING:
                    db_key = self.METADATA_MAPPING[key]
                    if db_key == 'title' and self.title == "Untitled":
                        self.title = value
                    self.metadata[db_key] = value

    def _parse(self):
        lines = self.raw_data.strip().split('\n')
        header_pattern = re.compile(r'^([A-Z]):\s*(.*)$')
        
        body_lines = []
        
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
                
            # Treat ANY line starting with [A-Z]: as a header
            match = header_pattern.match(line_stripped)
            if match:
                key, value = match.groups()
                # Strip trailing comments from header values
                value = re.sub(r'%.*', '', value).strip()
                
                if key in self.METADATA_MAPPING:
                    db_key = self.METADATA_MAPPING[key]
                    # Don't overwrite unless it's Title or we really want to
                    # titles can have multiple T: lines
                    if db_key == 'title':
                        if self.title == "Untitled":
                            self.title = value
                        else:
                            # Append to title? Some tunes have multiple T lines.
                            pass
                    self.metadata[db_key] = value
                continue
            
            if line_stripped.startswith('%'):
                continue
                
            # If it's not a header or comment, it might be body
            # We must be careful! Don't take "hornpipe" or HTML junk.
            # A music line usually has a high density of ABC chars and NO common English words.
            junk_words = ['tune', 'next', 'previous', 'sheet', 'music', 'rendered', 'last', 'updated', 'october', 'henrik', 'norbeck', 'cookies', 'adsense', 'adverts', 'consent', 'using', 'site']
            line_lower = line_stripped.lower()
            if any(word in line_lower for word in junk_words):
                continue

            abc_chars = len(re.findall(r'[a-gA-Gz0-9/|\[\]()_^=,\'~]', line_stripped))
            total_chars = len(line_stripped.replace(" ", ""))
            
            # Heuristic: line must be at least 80% ABC characters 
            # OR contain a bar line | and have no junk words
            if (total_chars > 0 and abc_chars / total_chars > 0.8) or ('|' in line_stripped and total_chars > 2):
                body_lines.append(line_stripped)
        
        self.tune_body = '\n'.join(body_lines)
        
        # Parse the body lines into individual musical elements
        body_text = ' '.join(body_lines)
        self._parse_body(body_text)

        # Calculate pitches using music21 if available
        # music21 is much more robust for complex ABC
        if MUSIC21_AVAILABLE:
            try:
                # We need to reconstruct a clean ABC for music21
                clean_abc = []
                # Add headers back
                # Important: Title and Key are minimal requirements for music21
                if 'reference_number' in self.metadata: clean_abc.append(f"X:{self.metadata['reference_number']}")
                else: clean_abc.append("X:1")
                
                for db_key, val in self.metadata.items():
                    # Find back the ABC key
                    abc_key = [k for k, v in self.METADATA_MAPPING.items() if v == db_key]
                    if abc_key and abc_key[0] != 'X':
                        clean_abc.append(f"{abc_key[0]}:{val}")
                
                clean_abc.append(f"K:{self.metadata.get('key', 'D')}") # Default to D if missing
                clean_abc.extend(body_lines)
                
                self.pitches = self.abc_to_pitches('\n'.join(clean_abc))
            except Exception as e:
                logger.warning(f"Music21 parsing failed for tune: {e}")
        
        # Fallback: extract pitches from elements if music21 failed, returned empty, or wasn't available
        if not self.pitches:
            self.pitches = self._extract_pitches_from_elements()

    def abc_to_pitches(self, abc_string):
        """
        Parse ABC notation and return a list of MIDI pitches using music21
        """
        try:
            score = converter.parse(abc_string, format='abc')
            pitches = []
            for n in score.recurse().notes:
                if isinstance(n, m21_note.Note):
                    pitches.append(n.pitch.midi)
            
            if pitches:
                return pitches
        except Exception as e:
            logger.debug(f"Music21 internal error: {e}")
            pass
        
        # Fallback: extract pitches from elements if music21 failed or returned empty
        # This is more robust for tunes with mixed documentation
        return self._extract_pitches_from_elements()

    def _extract_pitches_from_elements(self):
        """Fallback method to extract MIDI pitches from parsed elements"""
        pitches = []
        # Basic mapping for note names to MIDI relative to C4 (60)
        # This is a simplification; a full ABC parser is complex.
        # But we already have logic in _extract_pitch to get base note.
        # We need to handle octaves and accidentals better if we want accurate intervals.
        # For similarity search, we need relative intervals, so absolute pitch matters less
        # as long as intervals are correct. However, accurate MIDI is best.
        
        # Since implementing a full ABC->MIDI parser is complex, we will try to use
        # the simple relative pitch values based on C=0, D=2, etc?
        # No, let's stick to what we need: just non-empty logic to allow similarity potential.
        
        # Better fallback: Use the regex-based elements we already parsed
        for el in self.elements:
            if el['type'] == 'note':
                # Parse the note string to get approximate MIDI
                # This is a rough approximation but better than nothing
                val = el['value']
                try:
                    midi = self._abc_note_to_midi(val)
                    pitches.append(midi)
                except:
                    pass
        return pitches

    def _abc_note_to_midi(self, abc_note):
        # Handle accidentals
        acc_map = {'^': 1, '_': -1, '=': 0}
        acc = 0
        if abc_note.startswith('^'): acc = 1; abc_note = abc_note[1:]
        elif abc_note.startswith('_'): acc = -1; abc_note = abc_note[1:]
        elif abc_note.startswith('='): acc = 0; abc_note = abc_note[1:]
        
        # Handle octaves
        octave_adjust = 0
        while abc_note.endswith("'"): 
            octave_adjust += 12
            abc_note = abc_note[:-1]
        while abc_note.endswith(","): 
            octave_adjust -= 12
            abc_note = abc_note[:-1]
            
        # Base note (C=60 is middle C, but in ABC 'C' is usually C4 or C5 depending on K field which we ignore here for fallback)
        # ABC Standard: C is middle C ?? No.
        # C, = C3, C = C4, c = C5, c' = C6
        
        base_map = {
            'C': 60, 'D': 62, 'E': 64, 'F': 65, 'G': 67, 'A': 69, 'B': 71,
            'c': 72, 'd': 74, 'e': 76, 'f': 77, 'g': 79, 'a': 81, 'b': 83
        }
        
        base_val = base_map.get(abc_note, 60) # Default to C if weird
        return base_val + acc + octave_adjust

    def _parse_body(self, body_text):
        """
        Parse the tune body into individual musical elements.
        Elements include: notes, rests, accidentals, durations, octaves, etc.
        """
        # Remove comments (% to end of line)
        body_text = re.sub(r'%.*', '', body_text)
        
        # Token pattern for ABC notation
        token_pattern = r'''
            "[^"]+"                # Quoted strings (chord symbols, etc.)
            |[A-Z]:\s*[^ \n|]*      # Inline headers (K:, P:, L:, etc.)
            |\[[\w\s,]+\]           # Chords [CEG]
            |[_^=]?[a-gA-G]        # Note with optional accidental
            |z                     # Rest
            |[0-9]+(?:/[0-9]*)?    # Duration (2, /2, 3/2, etc.)
            |/                     # Duration shorthand
            |'                     # Octave up
            |,                     # Octave down
            |[()]                  # Ties
            |x                     # Invisible rest
            |~                     # Trill
            |![\w]+!               # Ornament/articulation
            |\|:                   # Repeat bar
            |:\|                   # Repeat bar
            |\|                    # Bar line
            |::                    # Double bar
            |\[[\d\|.,]+\]         # Alternate ending
        '''
        
        tokens = re.findall(token_pattern, body_text, re.VERBOSE)
        
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token.startswith('"') or (len(token) > 1 and token[1] == ':'):
                # Skip chord symbols and inline headers (like K: P: L:)
                pass
            elif token in ['|', '|:', ':|', '::', '[']:
                self.elements.append({'type': 'bar', 'value': token})
            elif token.startswith('[') and token.endswith(']'):
                if re.match(r'^\[\d', token):
                    self.elements.append({'type': 'alternate_ending', 'value': token})
                else:
                    self.elements.append({'type': 'chord', 'notes': token[1:-1]})
            elif token in ['z', 'x']:
                duration = self._get_next_duration(tokens, i)
                self.elements.append({'type': 'rest', 'value': token, 'duration': duration})
            elif re.match(r'^[_^=]?[a-gA-G]', token):
                note = token
                duration = self._get_next_duration(tokens, i)
                self.elements.append({'type': 'note', 'value': note, 'duration': duration, 'pitch': self._extract_pitch(note)})
            elif token in ['(', ')']:
                self.elements.append({'type': 'tie', 'value': token})
            elif token.startswith('!') and token.endswith('!'):
                self.elements.append({'type': 'ornament', 'value': token})
            elif token == '~':
                self.elements.append({'type': 'trill'})
            i += 1

    def _get_next_duration(self, tokens, current_index):
        if current_index + 1 < len(tokens):
            next_token = tokens[current_index + 1]
            if re.match(r'^[0-9]+(?:/[0-9]*)?$|^/$', next_token):
                return next_token
        return None

    def _extract_pitch(self, note):
        base = re.sub(r'^[_^=]', '', note)
        return base

    def to_dict(self):
        return {
            "title": self.title,
            "metadata": self.metadata,
            "raw_data": self.raw_data,
            "tune_body": self.tune_body,
            "pitches": ",".join(map(str, self.pitches)),
            "elements": self.elements,
            "status": self.status,
            "skip_reason": self.skip_reason
        }

class Tunebook:
    def __init__(self, url, content=None):
        self.url = url
        self.timestamp = datetime.now().isoformat()
        self.success = False
        self.tunes = []
        if content:
            self._parse_content(content)
        else:
            self._load_and_parse()

    def _load_and_parse(self):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT document FROM urls WHERE url = ?', (self.url,))
            row = cursor.fetchone()
            conn.close()
            
            if not row or not row[0]:
                self.success = False
                return
                
            content = row[0]
            if isinstance(content, bytes):
                content = content.decode('utf-8', errors='ignore')
            self._parse_content(content)
        except Exception as e:
            logger.error(f"Error loading and parsing URL {self.url}: {e}")
            self.success = False

    def _parse_content(self, content):
        try:
            # Normalize line endings: replace \r\n and \r with \n
            content = content.replace('\r\n', '\n').replace('\r', '\n')
            
            # HTML aware pre-processing: replace HTML tags with newlines to ensure X: headers
            # at the start of a line (even if they follow a <br> or are inside a <div>) are found.
            content = re.sub(r'<[^>]+>', '\n', content)
            
            # Stricter check: X: must be at the start of a line and followed by digits.
            # This filters out many false positives in minified JS/CSS or news text.
            if not re.search(r'(?m)^X:\s*\d+', content):
                self.success = False
                return

            # Secondary check: character distribution. If the page doesn't look like an ABC book, skip it.
            # Real ABC books usually have a high density of T: K: or bar lines |
            if not (re.search(r'(?m)^T:', content) or re.search(r'(?m)^K:', content) or content.count('|') > 5):
                 self.success = False
                 return

            self.success = True
            parts = re.split(r'(?m)^X:', content)
            
            # Limit the number of tunes parsed from a single URL to prevent hangs on garbage pages
            MAX_TUNES_PER_PAGE = 500
            if len(parts) > MAX_TUNES_PER_PAGE + 1:
                logger.warning(f"Too many potential tunes found ({len(parts)-1}) in {self.url}. Limiting to {MAX_TUNES_PER_PAGE}.")
                parts = parts[:MAX_TUNES_PER_PAGE + 1]

            for part in parts[1:]:
                tune_raw = "X:" + part
                try:
                    tune = Tune(tune_raw)
                    if tune.status != "skipped" or (tune.metadata and len(tune.metadata) > 1):
                        self.tunes.append(tune)
                except Exception as e:
                    logger.warning(f"Failed to parse individual tune in {self.url}: {e}")
            
            if not self.tunes:
                self.success = False
        except Exception as e:
            logger.error(f"Failed to parse ABC content in {self.url}: {e}")
            self.success = False

    def to_dict(self):
        return {
            "url": self.url,
            "timestamp": self.timestamp,
            "success": self.success,
            "tunes": [t.to_dict() for t in self.tunes]
        }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "No URL provided"}))
        sys.exit(1)

    url = sys.argv[1]
    tunebook = Tunebook(url)
    print(json.dumps(tunebook.to_dict()))

