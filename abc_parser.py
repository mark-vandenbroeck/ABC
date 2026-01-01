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
                    # Strip trailing comments from header values
                    value = re.sub(r'%.*', '', value).strip()
                    
                    if key in self.METADATA_MAPPING:
                        db_key = self.METADATA_MAPPING[key]
                        self.metadata[db_key] = value
                    
                    if key == 'T' and self.title == "Untitled":
                        self.title = value
                        
                    if key == 'K':
                        # Key is usually the last header line
                        in_header = False
                elif line.startswith('%'):
                    # Comments are allowed in headers, don't end header yet
                    continue
                else:
                    # Once we hit a line that isn't a header field or a comment, the tune body starts
                    in_header = False
                    body_lines.append(line)
            else:
                body_lines.append(line)
        
        self.tune_body = '\n'.join(body_lines)
        
        self.tune_body = '\n'.join(body_lines)
        
        # Parse the body lines into individual musical elements for internal representation
        body_text = ' '.join(body_lines)
        self._parse_body(body_text)

        # Calculate pitches using music21 if available
        if MUSIC21_AVAILABLE:
            try:
                self.pitches = self.abc_to_pitches(self.raw_data)
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
        except Exception:
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
            # Normalize line endings: replace \r\n and \r with \n
            content = content.replace('\r\n', '\n').replace('\r', '\n')
            
            # HTML aware pre-processing: replace HTML tags with newlines to ensure X: headers
            # at the start of a line (even if they follow a <br> or are inside a <div>) are found.
            content = re.sub(r'<[^>]+>', '\n', content)
            
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

