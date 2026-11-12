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
    snapshot_manifest = json.loads((root / 'snapshots.json').read_text())
    snapshots = []
    for entry in snapshot_manifest['files']:
        digest, rows = parse_manifest((root / 'snapshots' / entry['file']).read_bytes())
        snapshots.append({'file': entry['file'], 'digest': digest, 'rows': rows, 'declared_rows': entry['declared_rows'], 'declared_complete': entry['complete']})
    assert len(snapshots[0]['rows']) == 3 and len(snapshots[2]['rows']) == 1
    return {'available_snapshots': snapshots, 'legacy_delivery_count': len(parsed)}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
