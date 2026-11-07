"""Student guide: download population and weather sources before the ETL run.
This downloader does not download the SWITRS SQLite file. Existing valid files are normally reused."""
import argparse
import concurrent.futures
from contextlib import closing
import csv
import datetime
import getpass
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')
load_dotenv(ROOT / 'Python/.env')
load_dotenv()  # Load environment variables from .env file if present
DATA = ROOT / 'Original_Data'
ALLOWED_DOWNLOAD_HOSTS = {'www2.census.gov', 'noaa-ghcn-pds.s3.amazonaws.com'}
STATION_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,32}$')
DEFAULT_DB_NAME = 'test_warehouse'

PORTABLE_DEFAULTS = {
    'columns': {
        'case_id': 'case_id',
        'date': 'collision_date',
        'time': 'collision_time',
        'county': 'county_location',
        'severity': 'collision_severity',
        'lighting': 'lighting',
        'road_surface': 'road_surface',
        'reported_weather': 'weather_1',
        'killed': 'killed_victims',
        'injured': 'injured_victims',
        'latitude': 'latitude',
        'longitude': 'longitude',
    },
    'traffic_projection': [
        'case_id',
        'collision_date',
        'collision_severity',
        'collision_time',
        'county_location',
        'injured_victims',
        'killed_victims',
        'latitude',
        'lighting',
        'longitude',
        'party_count',
        'primary_collision_factor',
        'primary_road',
        'road_condition_1',
        'road_surface',
        'secondary_road',
        'type_of_collision',
        'weather_1',
    ],
    'longitude_positive_west': False,
    'max_station_distance_m': 50000,
    'batch_settings': {'daily_lookback_days': 2},
}


def validate_download_url(url):
    """Ensure only HTTPS URLs from approved public data hosts are opened."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != 'https' or parsed.netloc not in ALLOWED_DOWNLOAD_HOSTS:
        raise ValueError(f'Disallowed download URL: {url}')


def validate_station_id(station_id):
    """Ensure station identifiers cannot traverse directories or inject unexpected paths."""
    if not STATION_ID_PATTERN.fullmatch(station_id):
        raise ValueError(f'Invalid station identifier: {station_id}')
    return station_id


def sanitize_param(val, default=''):
    """Strip whitespace, quotes, and accidental trailing commas from connection settings."""
    if val is None:
        return default
    s = str(val).strip()
    s = s.strip("',\"").rstrip(',').strip()
    return s if s else default


def write_restricted_json(path, payload):
    """Write JSON configuration and restrict file permissions to the current user where supported."""
    path = Path(path)
    path.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def is_valid_local_file(path):
    """Check whether a local source file already exists and is non-empty."""
    path = Path(path)
    if not path.is_file() or path.stat().st_size <= 0:
        return False
    if path.name == 'ghcnd-inventory.txt' and path.stat().st_size < 30000000:
        return False
    return True


def download(url, path, force=False):
    """Reuse an existing file or download a missing file to a temporary path. Publish it only after the transfer completes; retry temporary failures."""
    validate_download_url(url)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not force and is_valid_local_file(path):
        print('Already saved:', path.name, flush=True)
        return path
    tmp = path.with_suffix(path.suffix + '.partial')
    request = urllib.request.Request(url, headers={'User-Agent': 'CaliforniaTrafficStudentProject/1.0'})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=60) as src, tmp.open('wb') as out:
                while chunk := src.read(65536):
                    out.write(chunk)
                expected = src.headers.get('Content-Length')
                if expected and out.tell() != int(expected):
                    raise IOError('Incomplete HTTP transfer')
            tmp.replace(path)
            print('Downloaded:', path.name, path.stat().st_size, flush=True)
            return path
        except Exception:
            if attempt == 2:
                tmp.unlink(missing_ok=True)
                raise
            time.sleep(2 * (attempt + 1))


def main():
    """Download missing Census and NOAA metadata, select nearby stations spanning the study years, and download only missing or new weather files."""
    DATA.mkdir(parents=True, exist_ok=True)
    files = {
        'co-est00int-tot.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv',
        'co-est2020-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv',
        'co-est2021-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/counties/totals/co-est2021-alldata.csv',
        'ghcnd-stations.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt',
        'ghcnd-inventory.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-inventory.txt',
    }
    missing_base = {name: url for name, url in files.items() if not is_valid_local_file(DATA / name)}
    if missing_base:
        print(f'Downloading {len(missing_base)} missing metadata/Census file(s)...', flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda kv: download(kv[1], DATA / kv[0]), missing_base.items()))
    else:
        print('All 5 base Census and NOAA metadata files already present; skipping re-download.', flush=True)

    selected_path = DATA / 'selected_stations.json'
    if is_valid_local_file(selected_path):
        selected_meta = json.loads(selected_path.read_text(encoding='utf-8'))
        selected = sorted( validate_station_id(sid) for sid in selected_meta )
        print(f'Reusing existing station selection ({len(selected)} stations).', flush=True)
    else:
        metadata = {}
        for line in (DATA / 'ghcnd-stations.txt').read_text(encoding='utf-8').splitlines():
            lat, lon = float(line[12:20]), float(line[21:30])
            if 32 <= lat <= 42.5 and -125 <= lon <= -113.5:
                sid = validate_station_id(line[:11].strip())
                metadata[sid] = {
                    'latitude': lat,
                    'longitude': lon,
                    'name': line[41:71].strip(),
                    'state': line[38:40],
                }

        coverage = {}
        for line in (DATA / 'ghcnd-inventory.txt').read_text(encoding='utf-8').splitlines():
            sid = line[:11].strip()
            element = line[31:35].strip()
            if sid in metadata and element in {'PRCP', 'TMIN', 'TMAX'}:
                first, last = int(line[36:40]), int(line[41:45])
                if first <= 2001 and last >= 2021:
                    coverage.setdefault(sid, set()).add(element)

        selected = sorted(sid for sid, elements in coverage.items() if elements == {'PRCP', 'TMIN', 'TMAX'})
        selected_path.write_text(
            json.dumps({sid: metadata[sid] for sid in selected}, indent=2),
            encoding='utf-8',
        )
        print('Selected', len(selected), 'stations: bounding region + all 3 elements spanning study years', flush=True)

    missing_stations = [
        sid for sid in selected if not is_valid_local_file(DATA / 'weather' / f'{sid}.csv.gz')
    ]
    already_saved_count = len(selected) - len(missing_stations)
    print(
        f'Station weather files: {already_saved_count} already saved, '
        f'{len(missing_stations)} missing to download.',
        flush=True,
    )

    def get_station(sid):
        """Build the public station download URL and save that station file in the weather folder."""
        validate_station_id(sid)
        return download(
            f'https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_station/{sid}.csv.gz',
            DATA / 'weather' / f'{sid}.csv.gz',
        )

    if missing_stations:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(get_station, missing_stations))

    # If config.json exists, also check if the SQLite source has newer dates/years requiring new Census or weather refreshes.
    config_path = ROOT / 'Support/Settings/config.json'
    if config_path.exists():
        cfg = json.loads(config_path.read_text(encoding='utf-8'))
        sqlite_file = Path(cfg.get('sqlite_path', ''))
        if sqlite_file.is_file():
            with closing(sqlite3.connect(sqlite_file.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                latest_year = int(
                    db.execute('SELECT max(substr(collision_date,1,4)) FROM collisions').fetchone()[0]
                )
                source_end = str(db.execute('SELECT max(collision_date) FROM collisions').fetchone()[0])[:10]
            snapshot_path = ROOT / 'Support/Generated/Checks/current_manifest.json'
            snapshot = (
                json.loads(snapshot_path.read_text(encoding='utf-8')) if snapshot_path.exists() else {}
            )
            previous_end = snapshot.get('source_end_date', '2021-06-03')
            refresh = cfg.get('weather_files', []) if source_end > previous_end else []
            ensure_sources(cfg, ROOT, latest_year, record=snapshot_path.exists(), refresh_paths=refresh)

    print('ALL DOWNLOADS COMPLETE', flush=True)


def record_download_catalog(project_root=None):
    """Record original file URLs, sizes, and checksums after a completed run.
    Student guide: record source URLs, file sizes, and SHA-256 checksums for reproducibility."""
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    manifest = json.loads((root / 'Support/Generated/Checks/current_manifest.json').read_text(encoding='utf-8'))
    urls = {
        'co-est00int-tot.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv',
        'co-est2020-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv',
        'co-est2021-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/counties/totals/co-est2021-alldata.csv',
        'co-est2025-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/co-est2025-alldata.csv',
        'ghcnd-stations.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt',
        'ghcnd-inventory.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-inventory.txt',
    }
    hashes = {Path(f['file']).resolve(): f['sha256'] for f in manifest['files']}
    records = []
    for file in sorted((root / 'Original_Data').rglob('*')):
        if not file.is_file() or file.name == 'demo.sqlite':
            continue
        digest = hashes.get(file.resolve())
        if not digest:
            h = hashlib.sha256()
            with file.open('rb') as f:
                for block in iter(lambda: f.read(1048576), b''):
                    h.update(block)
            digest = h.hexdigest()
        url = urls.get(file.name)
        if file.name.endswith('.csv.gz'):
            url = 'https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_station/' + file.name
        records.append(
            {
                'file': str(file.relative_to(root)),
                'bytes': file.stat().st_size,
                'sha256': digest,
                'source_url': url or 'Generated project crosswalk or station selection',
                'local_saved_utc': datetime.datetime.fromtimestamp(
                    file.stat().st_mtime, datetime.timezone.utc
                ).isoformat(),
            }
        )
    with (root / 'Support/Generated/Checks/Source_Catalog.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print('Recorded', len(records), 'data files with source URLs, sizes and checksums.')


def profile_traffic_source(project_root=None):
    """Read the original SQLite file without modifying it; save independent totals.
    Student guide: read the original SQLite source without modifying it.
    Save independent counts and totals used to validate the warehouse."""
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    config = json.loads((root / 'Support/Settings/config.json').read_text(encoding='utf-8'))
    uri = Path(config['sqlite_path']).resolve().as_uri() + '?mode=ro'
    out = {}
    with closing(sqlite3.connect(uri, uri=True)) as db:
        print('Profiling collision source...', flush=True)
        out['collisions'] = [
            dict(zip(['county', 'count', 'min_date', 'max_date', 'with_coordinates', 'killed', 'injured'], r))
            for r in db.execute(
                'SELECT county_location, count(*), min(collision_date), max(collision_date), '
                'sum(latitude IS NOT NULL AND longitude IS NOT NULL), '
                'sum(killed_victims), sum(injured_victims) '
                'FROM collisions GROUP BY county_location'
            )
        ]
        print('County/date profile complete', flush=True)
        out['severity'] = [
            list(r)
            for r in db.execute('SELECT collision_severity, count(*) FROM collisions GROUP BY collision_severity')
        ]
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        allowed_tables = ['collisions', 'parties', 'victims', 'case_ids']
        out['counts'] = {
            table: db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            for table in allowed_tables
            if table in tables
        }
    (root / 'Support/Generated/Checks/source_profile.json').write_text(
        json.dumps(out, indent=2), encoding='utf-8'
    )
    print(json.dumps(out, indent=2), flush=True)


def ensure_sources(config, project_root, end_year, record=True, refresh_paths=None):
    """Before ETL, restore missing public inputs and add published future population years.

    Existing files are reused unless a needed station is requested for refresh.
    A station is refreshed at most once per UTC day. Project mappings and station selection cannot be
    downloaded: they are our own inputs and must exist. No measurements are invented.
    """
    root = Path(project_root)
    data = root / 'Original_Data'
    for name in [config['county_crosswalk_path'], str(data / 'selected_stations.json')]:
        if not Path(name).is_file():
            raise FileNotFoundError('Project input must be restored from backup: ' + name)
    urls = {
        'co-est00int-tot.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv',
        'co-est2020-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv',
        'co-est2021-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/counties/totals/co-est2021-alldata.csv',
        'co-est2025-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/co-est2025-alldata.csv',
        'ghcnd-stations.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt',
    }
    # Keep historical releases stable; add only years missing from our study.
    published_year = 2025
    maximum = max(source['end_year'] for source in config['population_files'])
    target = min(int(end_year), published_year)
    if target > maximum:
        future = dict(
            path=str(data / 'co-est2025-alldata.csv'),
            vintage='2025',
            start_year=maximum + 1,
            end_year=target,
            encoding='cp1252',
        )
        config['population_files'].append(future)
    if int(end_year) > published_year:
        print(f'Population estimates are configured through {published_year}; later years stay missing.', flush=True)

    inputs = [(Path(config['station_metadata_path']), urls['ghcnd-stations.txt'])]
    for source in config['population_files']:
        path = Path(source['path'])
        if path.name not in urls:
            raise ValueError('No approved download URL for ' + str(path))
        inputs.append((path, urls[path.name]))
    for path in config['weather_files']:
        weather_path = Path(path)
        sid = weather_path.name.split('.')[0]
        validate_station_id(sid)
        inputs.append((weather_path, f'https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_station/{weather_path.name}'))

    downloaded = []
    refresh_paths = {str(Path(path).resolve()) for path in (refresh_paths or [])}
    refresh_history = config.setdefault('weather_refresh_checked_on', {})
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    for path, url in inputs:
        identity = str(path.resolve())
        refresh = identity in refresh_paths and refresh_history.get(identity) != today
        if path.is_file() and path.stat().st_size > 0 and not refresh:
            continue
        print(('New weather date; refreshing ' if refresh else 'Missing source; downloading ') + path.name, flush=True)
        download(url, path, force=refresh)
        # Validate file structure before the ETL uses a newly downloaded input.
        if path.name.endswith('.csv.gz'):
            with gzip.open(path, 'rt', encoding='utf-8') as file:
                first = file.readline().strip().split(',')
                if len(first) < 7 or first[0] != path.name.split('.')[0]:
                    path.unlink()
                    raise ValueError('Invalid NOAA station download: ' + str(path))
        elif path.name.startswith('co-est'):
            with path.open(encoding='cp1252', newline='') as file:
                fields = next(csv.reader(file))
            required = {'STATE', 'COUNTY', 'SUMLEV', 'CTYNAME'}
            required.update(
                'POPESTIMATE' + str(year)
                for source in config['population_files']
                if Path(source['path']) == path
                for year in range(source['start_year'], source['end_year'] + 1)
            )
            if not required.issubset(fields):
                path.unlink()
                raise ValueError('Invalid Census download: ' + str(path))
        with path.open('rb') as source:
            digest = hashlib.file_digest(source, 'sha256').hexdigest()
        downloaded.append(
            dict(
                file=str(path.resolve()),
                sha256=digest,
                bytes=path.stat().st_size,
                source_url=url,
            )
        )
        if path.name.endswith('.csv.gz'):
            refresh_history[identity] = today

    print(
        f'Source check: {len(inputs)} public files checked, {len(downloaded)} downloaded; existing files reused.',
        flush=True,
    )
    if downloaded:
        config['historical_reference_enabled'] = False
    if record and downloaded:
        # Record accepted replacement downloads, so a future full run verifies them.
        checks = root / 'Support/Generated/Checks'
        manifest_path = checks / 'current_manifest.json'
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            files = {str(Path(file['file']).resolve()): file for file in manifest.get('files', [])}
            files.update({file['file']: file for file in downloaded})
            manifest['files'] = list(files.values())
            manifest['full_validation_required'] = True
            manifest['status'] = 'sources_updated'
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')

        catalog_path = checks / 'Source_Catalog.csv'
        if catalog_path.exists():
            with catalog_path.open(encoding='utf-8-sig', newline='') as file:
                rows = list(csv.DictReader(file))
                columns = list(rows[0]) if rows else ['file', 'bytes', 'sha256', 'source_url', 'local_saved_utc']
            replaces = {file['file'] for file in downloaded}
            rows = [row for row in rows if str((root / row['file']).resolve()) not in replaces]
            for file in downloaded:
                rows.append(
                    dict(
                        file=str(Path(file['file']).relative_to(root)),
                        bytes=file['bytes'],
                        sha256=file['sha256'],
                        source_url=file['source_url'],
                        local_saved_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    )
                )
            with catalog_path.open('w', encoding='utf-8-sig', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
        # Persist release selection only after downloads succeed.
    if record:
        write_restricted_json(root / 'Support/Settings/config.json', config)
    return downloaded


def setup_project(sqlite_path=None, host='localhost', port=5432, database=DEFAULT_DB_NAME, user='postgres', password=None):
    """Create or update a portable project. On rerun, existing configs and downloaded files are reused,
    and only missing or new source files are downloaded.
    """
    import pandas as pd
    import psycopg
    from psycopg import sql

    config_path = ROOT / 'Support/Settings/config.json'
    existing_cfg = json.loads(config_path.read_text(encoding='utf-8')) if config_path.exists() else {}
    resolved_sqlite = sqlite_path or existing_cfg.get('sqlite_path')
    if not resolved_sqlite:
        raise ValueError('--sqlite is required when setting up for the first time')
    source = Path(resolved_sqlite).resolve()
    if not source.is_file():
        raise FileNotFoundError('SQLite file does not exist: ' + str(source))
    with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as db:
        columns = {row[1] for row in db.execute('PRAGMA table_info(collisions)')}
        if not {'case_id', 'collision_date', 'county_location'} <= columns:
            raise ValueError('Expected the SWITRS collisions table and columns')
    for name in [
        'Original_Data/weather',
        'Support/Settings',
        'Support/Generated/Staging',
        'Support/Generated/Checks',
        'Report',
    ]:
        (ROOT / name).mkdir(parents=True, exist_ok=True)

    db_cfg = existing_cfg.get('database', {})
    host = sanitize_param(os.getenv('PGHOST') or host or db_cfg.get('host'), 'localhost')
    port = int(sanitize_param(os.getenv('PGPORT') or port or db_cfg.get('port'), 5432))
    database = sanitize_param(os.getenv('PGDATABASE') or database or db_cfg.get('name'), DEFAULT_DB_NAME)
    user = sanitize_param(os.getenv('PGUSER') or user or db_cfg.get('user'), 'postgres')

    resolved_password = (
        password
        or os.getenv('TRAFFIC_PASSWORD')
        or os.getenv('PGPASSWORD')
        or db_cfg.get('password')
    )
    if not resolved_password:
        try:
            resolved_password = getpass.getpass(f'PostgreSQL password for user "{user}": ')
        except (EOFError, OSError):
            resolved_password = None

    if not resolved_password:
        raise ValueError(
            f'Password is required for PostgreSQL user "{user}". '
            'Please set PGPASSWORD in Python/.env, supply it via prompt, or pass --password.'
        )

    # Connect to the standard administration database before creating the target.
    with psycopg.connect(
        host=host, port=port, dbname='postgres', user=user, password=resolved_password, autocommit=True
    ) as db:
        if not db.execute('SELECT 1 FROM pg_database WHERE datname=%s', (database,)).fetchone():
            db.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
            print(f'Created PostgreSQL database: {database}', flush=True)
        else:
            print(f'Using existing PostgreSQL database: {database}', flush=True)

    main()

    crosswalk_path = DATA / 'county_crosswalk.csv'
    if not is_valid_local_file(crosswalk_path):
        census = pd.read_csv(DATA / 'co-est2021-alldata.csv', encoding='cp1252', dtype=str)
        census = census[(census.STATE == '06') & (census.SUMLEV == '050')]
        crosswalk = []
        for row in census.itertuples():
            name = row.CTYNAME.removesuffix(' County').strip().lower()
            crosswalk.append(
                {
                    'source_county': name,
                    'fips': row.STATE + row.COUNTY,
                    'provenance': 'County name normalized from Census vintage 2021',
                }
            )
        if len(crosswalk) != 58 or len({r['source_county'] for r in crosswalk}) != 58:
            raise ValueError('Expected 58 unique California counties')
        with crosswalk_path.open('w', encoding='utf-8', newline='') as out:
            writer = csv.DictWriter(out, fieldnames=['source_county', 'fips', 'provenance'])
            writer.writeheader()
            writer.writerows(crosswalk)
        print('Generated county_crosswalk.csv', flush=True)
    else:
        print('Reusing existing county_crosswalk.csv', flush=True)

    selected_stations = json.loads((DATA / 'selected_stations.json').read_text(encoding='utf-8'))
    config = PORTABLE_DEFAULTS.copy()
    config.update(existing_cfg)
    config.update(
        sqlite_path=str(source),
        traffic_release=existing_cfg.get('traffic_release', 'local SWITRS SQLite'),
        station_metadata_path=str(DATA / 'ghcnd-stations.txt'),
        county_crosswalk_path=str(crosswalk_path),
        weather_files=[str(DATA / 'weather' / f'{ validate_station_id(sid) }.csv.gz') for sid in selected_stations],
        population_files=existing_cfg.get(
            'population_files',
            [
                dict(path=str(DATA / name), vintage=vintage, start_year=start, end_year=end, encoding='cp1252')
                for name, vintage, start, end in [
                    ('co-est00int-tot.csv', '2000s-intercensal', 2001, 2009),
                    ('co-est2020-alldata.csv', '2020', 2010, 2020),
                    ('co-est2021-alldata.csv', '2021', 2021, 2021),
                ]
            ],
        ),
        historical_reference_enabled=False,
        database=dict(host=host, port=int(port), name=database, user=user, password=resolved_password),
    )
    write_restricted_json(config_path, config)
    print(f'Setup complete (database: {database}). Run: python Python/pipeline.py --all', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download weather and population source files')
    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        '--setup',
        action='store_true',
        help='Create or update folders, database, missing/new downloads, county mapping and config',
    )
    parser.add_argument('--sqlite', help='SWITRS SQLite path for setup')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--database', default=DEFAULT_DB_NAME)
    parser.add_argument('--user', default='postgres')
    parser.add_argument('--password', help='PostgreSQL password')
    action.add_argument('--catalog', action='store_true', help='Record source file checksums and URLs')
    action.add_argument('--profile', action='store_true', help='Read traffic source counts and totals')
    args = parser.parse_args()
    if args.setup:
        setup_project(args.sqlite, args.host, args.port, args.database, args.user, args.password)
    elif args.catalog:
        record_download_catalog()
    elif args.profile:
        profile_traffic_source()
    else:
        main()
