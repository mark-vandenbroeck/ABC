try:
    from music21 import converter, note as m21_note
    print("Music21 imported successfully")
except ImportError:
    print("Music21 not installed")
    exit(1)

abc_content = """X:1
T:Zocharti Loch
C:Louis Lewandowski (1821-1894)
K:Gm
M:C
Q:1/4=76
%            End of header, start of tune body:
% 1
[V:T1]  (B2c2 d2g2)  | f6e2      | (d2c2 d2)e2 | d4 c2z2 |
[V:T2]  (G2A2 B2e2)  | d6c2      | (B2A2 B2)c2 | B4 A2z2 |
[V:B1]       z8      | z2f2 g2a2 | b2z2 z2 e2  | f4 f2z2 |
[V:B2]       x8      |     x8    |      x8     |    x8   |
% 5
[V:T1]  (B2c2 d2g2)  | f8        | d3c (d2fe)  | H d6    ||
[V:T2]       z8      |     z8    | B3A (B2c2)  | H A6    ||
[V:B1]  (d2f2 b2e'2) | d'8       | g3g  g4     | H^f6    ||
[V:B2]       x8      | z2B2 c2d2 | e3e (d2c2)  | H d6    ||
This layout closely resembles printed music, and permits the
corresponding notes on different voices to be vertically aligned so that
the chords can be read directly from the abc. The addition of single
remark lines '
%
' between the grouped staves, indicating the bar
nummers, also makes the source more legible.
Here follows the visible output:
Here follows the audible output:
MIDI
V:
can appear both in the body and the header. In the latter case,
V:
is used exclusively to set voice properties. For example, the
name
property in the example above, specifies which label should be
printed on the first staff of the voice in question. Note that these
properties may be also set or changed in the tune body. The
V:
properties will be fully explained in the next section.
Please note that the exact grouping of voices on the staff or staves is
not specified by
V:
itself. This may be specified with the
%%score
stylesheet directive. See section
Voice grouping
for
details. Please see section
Instrumentation directives
to learn how to
assign a General MIDI instrument to a voice, using a
%%MIDI
stylesheet directive.
Although it is not recommended, the tune body of fragment"""

print("Attempting to parse...")
try:
    score = converter.parse(abc_content, format='abc')
    pitches = []
    for n in score.recurse().notes:
        if isinstance(n, m21_note.Note):
            pitches.append(n.pitch.midi)
    print(f"Parsed {len(pitches)} pitches.")
    print(pitches[:10])
except Exception as e:
    print(f"Parsing failed: {e}")
