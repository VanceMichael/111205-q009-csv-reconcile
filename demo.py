import json
from pathlib import Path
from service import parse_manifest


def run():
    root = Path(__file__).parent / 'fixtures'
    manifest = json.loads((root / 'deliveries.json').read_text())
    parsed = []
    for delivery in manifest['deliveries']:
        digest, rows = parse_manifest((root / 'inbox' / delivery['file']).read_bytes(), delivery['encoding'])
        assert digest == delivery['sha256'] and rows
        parsed.append({'file': delivery['file'], 'digest': digest, 'rows': rows})
    assert parsed[1]['digest'] == parsed[2]['digest'] != parsed[3]['digest']
    return {'observed_deliveries': parsed}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
