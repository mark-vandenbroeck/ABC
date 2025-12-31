from music21 import converter, note
def abc_to_pitches(abc_string):
    """
    Parse ABC notation and return a list of MIDI pitches
    """
    score = converter.parse(abc_string, format='abc')
    pitches = []

    for n in score.recurse().notes:
        if isinstance(n, note.Note):
            pitches.append(n.pitch.midi)

    return pitches

abc = """
X: 1
T:Aunt Hessie's White Horse
% Nottingham Music Database
S:Kevin Briggs
P:AAB
M:4/4
L:1/8
R:Hornpipe
K:G
P:A
d2|"G"G2A2 B2c2|"G"dd2d d2d2|"Em"dd2d d2d2|"D7"dd2d d2d2|"G"G2A2 B2c2|\
"G"dd2d d2d2|"D7"d2c2 B2A2|"G"G6:|
P:B
"G"g2g2 f2=f2|"C"ee2e e2e2|"C"g2g2 f2e2|"G"dd2d d2d2|"G"d2d2 e2d2|\
"D7"ff2f f2f2|"D7"d2c2 B2A2|"G"G6:|
"""

print(abc_to_pitches(abc))
