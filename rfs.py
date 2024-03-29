from datetime import datetime
from dateutil import tz
import io
from pathlib import Path
import os
import re
import subprocess
import sys
import tempfile

def parse_to_utc(date_string):
    date_none = datetime.fromisoformat(date_string)
    date_utc = datetime.replace(date_none, tzinfo=tz.UTC)
    return date_utc

p = Path(os.path.realpath("."))
fs = []
for f in sorted(p.glob('rfs-*.json.gz')):
    path = str(f)
    date_match = re.match(r'.*/rfs-(.*).json.gz$', path)
    date_str = date_match.group(1)
    date_syd = datetime.fromisoformat(date_str)
    date_utc = datetime.astimezone(date_syd, tz.UTC)
    fs.append({ "path": path, "date": date_utc })

# Find raws that we haven't added yet.
r = subprocess.run(
    [
        'psql', '-tq',
        '-c', 'CREATE TEMPORARY TABLE seen_raws(ts TIMESTAMP WITHOUT TIME ZONE)',
        '-c', 'COPY seen_raws(ts) FROM STDIN',
        '-c', '''
            SELECT TO_CHAR(ts, 'YYYY-MM-DD"T"HH24:MI')
              FROM (SELECT ts FROM seen_raws
                    EXCEPT (SELECT time_added from rfs.raw)) x
              ORDER BY ts
        ''',
    ],
    input='\n'.join([f["date"].strftime('%Y-%m-%d %H:%M') for f in fs]),
    encoding='ascii',
    capture_output=True,
)
if r.returncode != 0:
    print(r.stderr)
    sys.exit(1)
else:
    new_dates_utc = [parse_to_utc(ln.strip())
                     for ln in r.stdout.rstrip().split('\n')
                     if ln.strip() != '']
    new_dates_set = set(new_dates_utc)
    new_fs = []
    for f in fs:
        if f["date"] in new_dates_set:
            new_fs.append(f)

for f in new_fs:
    print(f'{f["path"]}')
    with tempfile.TemporaryFile() as json:
        subprocess.run(['zcat', f['path']], stdout=json)
        json.seek(0)
        subprocess.run([
            'psql', '--quiet',
            '-c', 'CREATE TEMPORARY TABLE rfs_raw_temp(j jsonb)',
            '-c', 'COPY rfs_raw_temp(j) FROM STDIN',
            '-c', f'''
                WITH ins AS (
                    INSERT INTO rfs.raw (rfs, time_added)
                         SELECT j, '{f["date"]}'::TIMESTAMP WITH TIME ZONE
                           FROM rfs_raw_temp
                    ON CONFLICT (time_added) DO NOTHING
                      RETURNING id
                )
                SELECT rfs.add_new_fires(id) FROM ins;
        '''], stdin=json)
