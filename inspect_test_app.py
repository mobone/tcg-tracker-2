from pathlib import Path

content = Path('test_app.py').read_bytes()
print(repr(content[:200]))
