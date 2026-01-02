
import logging
from abc_parser import Tune

logging.basicConfig(level=logging.INFO)

def test_skip():
    # Large tune test
    large_abc = "X:1\nT:Large Tune\n" + ("C" * 11000)
    tune = Tune(large_abc)
    print(f"Status: {tune.status}")
    print(f"Reason: {tune.skip_reason}")
    print(f"Title: {tune.title}")

    # Complex tune test
    complex_abc = "X:2\nT:Complex Tune\n" + "V:1\nC\nV:2\nD\nV:3\nE\nV:4\nF\nV:5\nG\n"
    tune2 = Tune(complex_abc)
    print(f"Status: {tune2.status}")
    print(f"Reason: {tune2.skip_reason}")
    print(f"Title: {tune2.title}")

if __name__ == "__main__":
    test_skip()
