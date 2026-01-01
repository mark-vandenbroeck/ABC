import sys
import logging
logging.basicConfig(level=logging.INFO)

try:
    from abc_parser import Tune
    print("Imported Tune class successfully")
except ImportError as e:
    print(f"Import failed: {e}")
    sys.exit(1)

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
"""

print("--- Testing Parse ---")
tune = Tune(abc_content)
print(f"Title: {tune.title}")
print(f"Pitches count: {len(tune.pitches)}")
print(f"Elements count: {len(tune.elements)}")
print(f"Pitches: {tune.pitches}")
