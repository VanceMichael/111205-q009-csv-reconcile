import csv
import hashlib
import io

def parse_manifest(content, encoding='utf-8'):
    digest = hashlib.sha256(content).hexdigest()
    text = content.decode(encoding)
    rows = list(csv.DictReader(io.StringIO(text, newline='')))
    return digest, rows
