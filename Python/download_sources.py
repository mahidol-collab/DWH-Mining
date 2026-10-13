"""Student guide: download population and weather sources before the ETL run.
This downloader does not download the SWITRS SQLite file. Existing valid files are normally reused."""
from pathlib import Path
import urllib.request
import time
import concurrent.futures
import json
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'Original_Data'
DATA.mkdir(parents=True, exist_ok=True)

def download(url, path, force=False):
    """Reuse an existing file or download a missing file to a temporary path. Publish it only after the transfer completes; retry temporary failures."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not force and path.exists() and path.stat().st_size > 0 and (not (path.name == 'ghcnd-inventory.txt' and path.stat().st_size < 30000000)):
        print('Already saved:', path.name, flush=True)
        return path
    tmp = path.with_suffix(path.suffix + '.partial')
    for attempt in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'CaliforniaTrafficStudentProject/1.0'}), timeout=60) as src, tmp.open('wb') as out:
                while (chunk := src.read(65536)):
                    out.write(chunk)
                expected = src.headers.get('Content-Length')
                if expected and out.tell() != int(expected):
                    raise IOError('Incomplete HTTP transfer')
            tmp.replace(path)
            print('Downloaded:', path.name, path.stat().st_size, flush=True)
            return path
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))

def main():
    """Download Census and NOAA metadata, select nearby stations spanning the study years, and download their weather files."""
    files = {'co-est00int-tot.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv', 'co-est2020-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv', 'co-est2021-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/counties/totals/co-est2021-alldata.csv', 'ghcnd-stations.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt', 'ghcnd-inventory.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-inventory.txt'}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda kv: download(kv[1], DATA / kv[0]), files.items()))
    metadata = {}
    for line in (DATA / 'ghcnd-stations.txt').read_text(encoding='utf-8').splitlines():
        lat, lon = (float(line[12:20]), float(line[21:30]))
        if 32 <= lat <= 42.5 and -125 <= lon <= -113.5:
            metadata[line[:11]] = {'latitude': lat, 'longitude': lon, 'name': line[41:71].strip(), 'state': line[38:40]}
    coverage = {}
    for line in (DATA / 'ghcnd-inventory.txt').read_text(encoding='utf-8').splitlines():
        sid = line[:11]
        element = line[31:35].strip()
        if sid in metadata and element in {'PRCP', 'TMIN', 'TMAX'}:
            first, last = (int(line[36:40]), int(line[41:45]))
            if first <= 2001 and last >= 2021:
                coverage.setdefault(sid, set()).add(element)
    selected = sorted((sid for sid, elements in coverage.items() if elements == {'PRCP', 'TMIN', 'TMAX'}))
    (DATA / 'selected_stations.json').write_text(json.dumps({sid: metadata[sid] for sid in selected}, indent=2), encoding='utf-8')
    print('Selected', len(selected), 'stations: bounding region + all 3 elements spanning study years', flush=True)

    def get_station(sid):
        """Build the public station download URL and save that station file in the weather folder."""
        return download('https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_station/' + sid + '.csv.gz', DATA / 'weather' / (sid + '.csv.gz'))
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(get_station, selected))
    print('ALL DOWNLOADS COMPLETE', flush=True)


def record_download_catalog(project_root=None):
    """Record original file URLs, sizes, and checksums after a completed run."""
    """Student guide: record source URLs, file sizes, and SHA-256 checksums for reproducibility."""
    from pathlib import Path
    import json, csv, datetime, hashlib
    ROOT = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    manifest = json.loads((ROOT / 'Support/Generated/Checks/current_manifest.json').read_text())
    urls = {'co-est00int-tot.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv', 'co-est2020-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv', 'co-est2021-alldata.csv': 'https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/counties/totals/co-est2021-alldata.csv', 'ghcnd-stations.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt', 'ghcnd-inventory.txt': 'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-inventory.txt'}
    hashes = {Path(f['file']).resolve(): f['sha256'] for f in manifest['files']}
    records = []
    for file in sorted((ROOT / 'Original_Data').rglob('*')):
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
        records.append({'file': str(file.relative_to(ROOT)), 'bytes': file.stat().st_size, 'sha256': digest, 'source_url': url or 'Generated project crosswalk or station selection', 'local_saved_utc': datetime.datetime.fromtimestamp(file.stat().st_mtime, datetime.timezone.utc).isoformat()})
    with (ROOT / 'Support/Generated/Checks/Source_Catalog.csv').open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=records[0].keys())
        w.writeheader()
        w.writerows(records)
    print('Recorded', len(records), 'data files with source URLs, sizes and checksums.')


def profile_traffic_source(project_root=None):
    """Read the original SQLite file without modifying it; save independent totals."""
    """Student guide: read the original SQLite source without modifying it.
    Save independent counts and totals used to validate the warehouse."""
    import sqlite3, json
    from pathlib import Path
    ROOT = Path(project_root) if project_root else Path(__file__).resolve().parents[1]
    config = json.loads((ROOT / 'Support/Settings/config.json').read_text())
    db = sqlite3.connect(Path(config['sqlite_path']).resolve().as_uri() + '?mode=ro', uri=True)
    out = {}
    print('Profiling collision source...', flush=True)
    out['collisions'] = [dict(zip(['county', 'count', 'min_date', 'max_date', 'with_coordinates', 'killed', 'injured'], r)) for r in db.execute('SELECT county_location,count(*),min(collision_date),max(collision_date),sum(latitude IS NOT NULL AND longitude IS NOT NULL),sum(killed_victims),sum(injured_victims) FROM collisions GROUP BY county_location')]
    print('County/date profile complete', flush=True)
    out['severity'] = [list(r) for r in db.execute('SELECT collision_severity,count(*) FROM collisions GROUP BY collision_severity')]
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    out['counts'] = {table: db.execute('SELECT count(*) FROM ' + table).fetchone()[0] for table in ['collisions', 'parties', 'victims', 'case_ids'] if table in tables}
    db.close()
    (ROOT / 'Support/Generated/Checks/source_profile.json').write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2), flush=True)


def ensure_sources(config, project_root, end_year, record=True, refresh_paths=None):
    """Before ETL, restore missing public inputs and add published future population years.

    Existing files are reused unless a needed station is requested for refresh.
    A station is refreshed at most once per UTC day. Project mappings and station selection cannot be
    downloaded: they are our own inputs and must exist. No measurements are invented.
    """
    import hashlib, csv, datetime
    root = Path(project_root)
    data = root / 'Original_Data'
    for name in [config['county_crosswalk_path'], str(data/'selected_stations.json')]:
        if not Path(name).is_file():
            raise FileNotFoundError('Project input must be restored from backup: '+name)
    urls = {
        'co-est00int-tot.csv':'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv',
        'co-est2020-alldata.csv':'https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv',
        'co-est2021-alldata.csv':'https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/counties/totals/co-est2021-alldata.csv',
        'co-est2025-alldata.csv':'https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/co-est2025-alldata.csv',
        'ghcnd-stations.txt':'https://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt'
    }
    # Keep historical releases stable; add only years missing from our study.
    published_year = 2025
    maximum = max(source['end_year'] for source in config['population_files'])
    target = min(int(end_year), published_year)
    if target > maximum:
        future = dict(path=str(data/'co-est2025-alldata.csv'), vintage='2025',
                      start_year=maximum+1, end_year=target, encoding='cp1252')
        config['population_files'].append(future)
    if int(end_year)>published_year:
        print(f'Population estimates are configured through {published_year}; later years stay missing.',flush=True)
    inputs = [(Path(config['station_metadata_path']),urls['ghcnd-stations.txt'])]
    for source in config['population_files']:
        path=Path(source['path'])
        if path.name not in urls:
            raise ValueError('No approved download URL for '+str(path))
        inputs.append((path,urls[path.name]))
    inputs += [(Path(path),'https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_station/'+Path(path).name) for path in config['weather_files']]
    downloaded=[]
    refresh_paths={str(Path(path).resolve()) for path in (refresh_paths or [])}
    refresh_history=config.setdefault('weather_refresh_checked_on',{})
    today=datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    for path,url in inputs:
        identity=str(path.resolve())
        refresh=identity in refresh_paths and refresh_history.get(identity)!=today
        if path.is_file() and path.stat().st_size>0 and not refresh:
            continue
        print(('New weather date; refreshing ' if refresh else 'Missing source; downloading ')+path.name,flush=True)
        download(url,path,force=refresh)
        # Validate file structure before the ETL uses a newly downloaded input.
        if path.name.endswith('.csv.gz'):
            import gzip
            with gzip.open(path,'rt') as file:
                first=file.readline().strip().split(',')
                if len(first)<7 or first[0]!=path.name.split('.')[0]:
                    path.unlink()
                    raise ValueError('Invalid NOAA station download: '+str(path))
        elif path.name.startswith('co-est'):
            with path.open(encoding='cp1252',newline='') as file:
                fields=next(csv.reader(file))
            required={'STATE','COUNTY','SUMLEV','CTYNAME'}
            required.update('POPESTIMATE'+str(year) for source in config['population_files'] if Path(source['path'])==path for year in range(source['start_year'],source['end_year']+1))
            if not required.issubset(fields):
                path.unlink()
                raise ValueError('Invalid Census download: '+str(path))
        with path.open('rb') as source:
            digest=hashlib.file_digest(source,'sha256').hexdigest()
        downloaded.append(dict(file=str(path.resolve()),sha256=digest,bytes=path.stat().st_size,source_url=url))
        if path.name.endswith('.csv.gz'):
            refresh_history[identity]=today
    print(f'Source check: {len(inputs)} public files checked, {len(downloaded)} downloaded; existing files reused.',flush=True)
    if downloaded:
        config['historical_reference_enabled']=False
    if record and downloaded:
        # Record accepted replacement downloads, so a future full run verifies them.
        checks=root/'Support/Generated/Checks'
        manifest_path=checks/'current_manifest.json'
        manifest=json.loads(manifest_path.read_text())
        files={str(Path(file['file']).resolve()):file for file in manifest['files']}
        files.update({file['file']:file for file in downloaded})
        manifest['files']=list(files.values())
        manifest['full_validation_required']=True
        manifest['status']='sources_updated'
        manifest_path.write_text(json.dumps(manifest,indent=2))
        catalog_path=checks/'Source_Catalog.csv'
        with catalog_path.open(encoding='utf-8-sig',newline='') as file:
            rows=list(csv.DictReader(file));columns=list(rows[0])
        replaces={file['file'] for file in downloaded}
        rows=[row for row in rows if str((root/row['file']).resolve()) not in replaces]
        for file in downloaded:
            rows.append(dict(file=str(Path(file['file']).relative_to(root)),bytes=file['bytes'],sha256=file['sha256'],source_url=file['source_url'],local_saved_utc=datetime.datetime.now(datetime.timezone.utc).isoformat()))
        with catalog_path.open('w',encoding='utf-8-sig',newline='') as file:
            writer=csv.DictWriter(file,fieldnames=columns);writer.writeheader();writer.writerows(rows)
        # Persist release selection only after downloads succeed.
    if record:
        (root/'Support/Settings/config.json').write_text(json.dumps(config,indent=2))
    return downloaded


PORTABLE_DEFAULTS = {'columns': {'case_id': 'case_id', 'date': 'collision_date', 'time': 'collision_time', 'county': 'county_location', 'severity': 'collision_severity', 'lighting': 'lighting', 'road_surface': 'road_surface', 'reported_weather': 'weather_1', 'killed': 'killed_victims', 'injured': 'injured_victims', 'latitude': 'latitude', 'longitude': 'longitude'}, 'traffic_projection': ['case_id', 'collision_date', 'collision_severity', 'collision_time', 'county_location', 'injured_victims', 'killed_victims', 'latitude', 'lighting', 'longitude', 'party_count', 'primary_collision_factor', 'primary_road', 'road_condition_1', 'road_surface', 'secondary_road', 'type_of_collision', 'weather_1'], 'longitude_positive_west': False, 'max_station_distance_m': 50000, 'batch_settings': {'daily_lookback_days': 2}}

def setup_project(sqlite_path, host='localhost', port=5432, database='warehouse', user='postgres'):
    """Create a fresh portable project. Existing configs are left intact.
    Run this once after copying the two scripts and installing requirements.
    NOAA selection and the Census county mapping are generated from public files.
    """
    import sqlite3, csv, getpass
    import psycopg
    from psycopg import sql
    source = Path(sqlite_path).resolve()
    if not source.is_file():
        raise FileNotFoundError('SQLite file does not exist: '+str(source))
    config_path = ROOT/'Support/Settings/config.json'
    if config_path.exists():
        raise FileExistsError('Setup already exists. Use pipeline.py --all, or edit the existing config.')
    with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as db:
        columns = {row[1] for row in db.execute('PRAGMA table_info(collisions)')}
        if not {'case_id','collision_date','county_location'} <= columns:
            raise ValueError('Expected the SWITRS collisions table and columns')
    for name in ['Original_Data/weather','Support/Settings','Support/Generated/Staging','Support/Generated/Checks','Report']:
        (ROOT/name).mkdir(parents=True,exist_ok=True)
    password = getpass.getpass('PostgreSQL password to save in local config: ')
    # Connect to the standard administration database before creating the target.
    with psycopg.connect(host=host,port=port,dbname='postgres',user=user,password=password,autocommit=True) as db:
        if not db.execute('SELECT 1 FROM pg_database WHERE datname=%s',(database,)).fetchone():
            db.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
    main()
    import pandas as pd
    census = pd.read_csv(DATA/'co-est2021-alldata.csv',encoding='cp1252',dtype=str)
    census = census[(census.STATE=='06') & (census.SUMLEV=='050')]
    crosswalk = []
    for row in census.itertuples():
        name = row.CTYNAME.removesuffix(' County').strip().lower()
        crosswalk.append({'source_county':name,'fips':row.STATE+row.COUNTY,'provenance':'County name normalized from Census vintage 2021'})
    if len(crosswalk)!=58 or len({r['source_county'] for r in crosswalk})!=58:
        raise ValueError('Expected 58 unique California counties')
    with (DATA/'county_crosswalk.csv').open('w',encoding='utf-8',newline='') as out:
        writer=csv.DictWriter(out,fieldnames=['source_county','fips','provenance']);writer.writeheader();writer.writerows(crosswalk)
    config = PORTABLE_DEFAULTS.copy()
    config.update(sqlite_path=str(source),traffic_release='local SWITRS SQLite',
        station_metadata_path=str(DATA/'ghcnd-stations.txt'),
        county_crosswalk_path=str(DATA/'county_crosswalk.csv'),
        weather_files=[str(DATA/'weather'/(sid+'.csv.gz')) for sid in json.loads((DATA/'selected_stations.json').read_text(encoding='utf-8'))],
        population_files=[dict(path=str(DATA/name),vintage=vintage,start_year=start,end_year=end,encoding='cp1252') for name,vintage,start,end in [
            ('co-est00int-tot.csv','2000s-intercensal',2001,2009),
            ('co-est2020-alldata.csv','2020',2010,2020),
            ('co-est2021-alldata.csv','2021',2021,2021)]],
        historical_reference_enabled=False,
        database=dict(host=host,port=int(port),name=database,user=user,password=password))
    config_path.write_text(json.dumps(config,indent=2)+'\n',encoding='utf-8')
    print('Setup complete. Run: python Python/pipeline.py --all',flush=True)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Download weather and population source files')
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--setup', action='store_true', help='Create fresh folders, database, downloads, county mapping and config')
    parser.add_argument('--sqlite', help='SWITRS SQLite path for setup')
    parser.add_argument('--host',default='localhost')
    parser.add_argument('--port',type=int,default=5432)
    parser.add_argument('--database',default='warehouse')
    parser.add_argument('--user',default='postgres')
    action.add_argument('--catalog', action='store_true', help='Record source file checksums and URLs')
    action.add_argument('--profile', action='store_true', help='Read traffic source counts and totals')
    args = parser.parse_args()
    if args.setup:
        if not args.sqlite:parser.error('--setup requires --sqlite')
        setup_project(args.sqlite,args.host,args.port,args.database,args.user)
    elif args.catalog:
        record_download_catalog()
    elif args.profile:
        profile_traffic_source()
    else:
        main()
