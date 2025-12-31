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

    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.metadata = {}
        self.elements = []
        self.title = "Untitled"
        self.tune_body = ""
        self.pitches = []
        self._parse()

    def _parse(self):
        lines = self.raw_data.strip().split('\n')
        header_pattern = re.compile(r'^([A-Z]):\s*(.*)$')
        
        in_header = True
        body_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if in_header:
                match = header_pattern.match(line)
                if match:
                    key, value = match.groups()
                    if key in self.METADATA_MAPPING:
                        db_key = self.METADATA_MAPPING[key]
                        self.metadata[db_key] = value
                    
                    if key == 'T' and self.title == "Untitled":
                        self.title = value
                else:
                    # Once we hit a line that isn't a header field, body starts
                    # (Note: ABC standard specifies K field ends header)
                    in_header = False
                    body_lines.append(line)
            else:
                body_lines.append(line)
        
        self.tune_body = '\n'.join(body_lines)
        
        # Calculate pitches using music21 if available
        if MUSIC21_AVAILABLE:
            try:
                self.pitches = self.abc_to_pitches(self.raw_data)
            except Exception as e:
                logger.warning(f"Music21 parsing failed for tune: {e}")
        
        # Parse the body lines into individual musical elements for internal representation
        body_text = ' '.join(body_lines)
        self._parse_body(body_text)

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
            return pitches
        except Exception:
            # Silently fail for pitches calculation if music21 fails on complex/invalid ABC
            return []

    def _parse_body(self, body_text):
        """
        Parse the tune body into individual musical elements.
        Elements include: notes, rests, accidentals, durations, octaves, etc.
        """
        # Remove comments (% to end of line)
        body_text = re.sub(r'%.*', '', body_text)
        
        # Token pattern for ABC notation
        token_pattern = r'''
            \[[\w\s,]+\]           # Chords [CEG]
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
            if token in ['|', '|:', ':|', '::', '[']:
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
            "elements": self.elements
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
            # Simple check if it looks like ABC
            if "X:" not in content:
                self.success = False
                return

            self.success = True
            parts = re.split(r'(?m)^X:', content)
            for part in parts[1:]:
                tune_raw = "X:" + part
                try:
                    tune = Tune(tune_raw)
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

