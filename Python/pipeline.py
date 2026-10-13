"""Student guide: this file contains the complete Extract, Transform, and Load process.
Start with main() at the bottom. Follow the numbered sections for each step.
Original files stay unchanged. All prepared outputs are saved in Support/Generated/Staging/."""
from pathlib import Path
from contextlib import closing
import sys, os, json, csv, sqlite3, datetime as dt, time, hashlib, argparse, uuid
ROOT = Path(__file__).resolve().parents[1]
if sys.version_info[:2] == (3, 12):
    sys.path.insert(0, str(ROOT / 'Support/libraries/active'))
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
import psycopg
CONFIG_PATH = ROOT / 'Support/Settings/config.json'
if not CONFIG_PATH.exists():
    raise SystemExit('Run Python/download_sources.py --setup --sqlite <your SWITRS file> first.')
CFG = json.loads(CONFIG_PATH.read_text(encoding='utf-8'))
OUT = ROOT / 'Support/Generated/Staging'
OUT.mkdir(parents=True, exist_ok=True)
(ROOT / 'Support/Generated/Checks').mkdir(parents=True, exist_ok=True)
EARTH_RADIUS = 6378168.0
START = '2001-01-01'
END = '2021-06-03'
FIELDS = list(CFG['columns'])
FACT_COLS = [
    'case_id',
    'date_key',
    'time_key',
    'county_key',
    'context_key',
    'source_key',
    'severity',
    'killed',
    'injured',
    'collision_count',
    'latitude',
    'longitude',
    'minute_of_day',
    'batch_id',
    'primary_road',
    'secondary_road',
    'primary_collision_factor',
    'type_of_collision',
]
METRICS = {}
TIMINGS = {}
ALLOW_TRAFFIC_UPDATES = False
TRAFFIC_CHANGED = False

def log(message):
    """Print a timestamped progress message so the full run can be followed."""
    print(f'{dt.datetime.now(dt.timezone.utc).isoformat()} {message}', flush=True)

def database_password():
    """Use an environment override or the local config; ask only if both are empty."""
    password = os.getenv('TRAFFIC_PASSWORD') or CFG.get('database', {}).get('password')
    if password:
        return password
    import getpass
    return getpass.getpass('PostgreSQL password: ')


def public_config():
    """Record useful settings in the run manifest without copying the password."""
    settings = dict(CFG)
    settings['database'] = {key: value for key, value in CFG.get('database', {}).items() if key != 'password'}
    return settings


def connect():
    """Connect using config settings, with optional environment overrides."""
    database = CFG.get('database', {})
    return psycopg.connect(
        host=os.getenv('PGHOST') or database.get('host', 'localhost'),
        port=int(os.getenv('PGPORT') or database.get('port', 5432)),
        dbname=os.getenv('PGDATABASE') or database.get('name', 'warehouse'),
        user=os.getenv('PGUSER') or database.get('user', 'postgres'),
        password=database_password(),
        autocommit=True,
    )


def save(frame, name, append=False):
    """Save a prepared table as CSV. Empty CSV fields represent missing values; append traffic chunks after the first chunk."""
    frame.to_csv(OUT / (name + '.csv'), index=False, mode='a' if append else 'w', header=not append, na_rep='', lineterminator='\n')

def number(s):
    """Convert valid finite numeric text to numbers. Invalid text and infinity become missing values."""
    text = s.astype('string').str.strip()
    valid = text.str.fullmatch('[+-]?[0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?', na=False)
    n = pd.to_numeric(text.where(valid), errors='coerce').astype('float64')
    return n.where(np.isfinite(n))

def category(s):
    """Trim spaces, use lowercase labels, and represent blank categories as unknown."""
    return s.astype('string').str.strip().str.lower().replace('', pd.NA).fillna('unknown')

def nonnegative_count(s):
    """Keep only nonnegative whole-number counts. Missing or invalid counts must not become zero."""
    n = number(s)
    return n.where((n >= 0) & (n < 2 ** 63) & (n == np.floor(n))).astype('Int64')

def clean_time(s):
    """Return the hour key and minutes since midnight. Use hour key -1 for an unknown or invalid time."""
    text = s.astype('string').str.strip()
    numeric = text.str.fullmatch('\\d{1,4}', na=False)
    colon = text.str.fullmatch('\\d{2}:\\d{2}(?::\\d{2})?', na=False)
    hhmm = text.where(numeric).str.zfill(4)
    hhmm = hhmm.where(~colon, text.str.slice(0, 5).str.replace(':', '', regex=False))
    h = pd.to_numeric(hhmm.str.slice(0, 2), errors='coerce')
    m = pd.to_numeric(hhmm.str.slice(2, 4), errors='coerce')
    ok = (h >= 0) & (h < 24) & (m >= 0) & (m < 60)
    return (h.where(ok, -1).astype('int32'), (h * 60 + m).where(ok).astype('Int64'))

def clean_traffic(raw, start_date=None, end_date=None):
    """Clean one collision chunk and return accepted rows separately from rows with invalid IDs or study dates."""
    rename = {source: logical for logical, source in CFG['columns'].items()}
    c = raw.rename(columns=rename).copy()
    c['case_id'] = c['case_id'].astype('string').str.strip()
    c['full_date'] = pd.to_datetime(c['date'], format='%Y-%m-%d', errors='coerce')
    bad_id = c['case_id'].isna() | c['case_id'].eq('')
    bad_day = c['full_date'].isna() | ~c['full_date'].between(start_date or START, end_date or END)
    c['reject_reason'] = np.select([bad_id.to_numpy(dtype=bool), bad_day.to_numpy(dtype=bool)], ['missing_case_id', 'invalid_or_out_of_scope_date'], default='')
    for k in ['severity', 'lighting', 'road_surface', 'reported_weather']:
        c[k] = category(c[k])
    c['time_key'], c['minute_of_day'] = clean_time(c['time'])
    for k in ['killed', 'injured']:
        c[k] = nonnegative_count(c[k])
    lat = number(c['latitude'])
    lon = number(c['longitude'])
    if CFG['longitude_positive_west']:
        lon = -lon.abs()
    good = lat.between(32, 42.5) & lon.between(-125, -113.5)
    c['latitude'] = lat.where(good)
    c['longitude'] = lon.where(good)
    c['geocoded'] = good
    c['date_key'] = c['full_date'].dt.strftime('%Y%m%d').astype('Int64')
    c['year'] = c['full_date'].dt.year.astype('Int64')
    c['month'] = c['full_date'].dt.month.astype('Int64')
    c['iso_weekday'] = (c['full_date'].dt.dayofweek + 1).astype('Int64')
    c['is_weekend'] = c['iso_weekday'] > 5
    return (c[c['reject_reason'].eq('')].copy(), c[~c['reject_reason'].eq('')].copy())

def clean_population(write_outputs=True):
    """Select California counties, turn year columns into rows, choose one release per county-year, and save county and population tables."""
    records = []
    for src in CFG['population_files']:
        with open(src['path'], encoding=src.get('encoding', 'cp1252'), newline='') as f:
            for r in csv.DictReader(f):
                if r['STATE'].zfill(2) != '06' or r['SUMLEV'].zfill(3) != '050':
                    continue
                for year in range(src['start_year'], src['end_year'] + 1):
                    value = int(r[f'POPESTIMATE{year}'])
                    if value <= 0:
                        raise ValueError('Population must be positive')
                    records.append((r['STATE'].zfill(2) + r['COUNTY'].zfill(3), r['CTYNAME'], year, value, src['vintage']))
    pop = pd.DataFrame(records, columns=['fips', 'county_name', 'year_key', 'population', 'vintage'])
    if pop.duplicated(['fips', 'year_key']).any():
        raise ValueError('Duplicate selected county-year')
    expected_years = sum(source['end_year']-source['start_year']+1 for source in CFG['population_files'])
    assert len(pop) == 58*expected_years and pop.groupby('year_key').size().eq(58).all()
    counties = pop.sort_values('year_key').drop_duplicates('fips', keep='last')[['fips', 'county_name']].sort_values('fips').reset_index(drop=True)
    counties.insert(0, 'county_key', np.arange(1, len(counties) + 1))
    counties['state_name'] = 'California'
    unknown = pd.DataFrame([[0, 'UNKNOWN', 'unknown', 'California']], columns=counties.columns)
    counties = pd.concat([unknown, counties], ignore_index=True)
    pop = pop.merge(counties[['fips', 'county_key']], on='fips', validate='many_to_one')
    pop['source_key'] = 3
    if write_outputs:
        save(counties, 'dim_county')
        save(pop[['county_key', 'year_key', 'source_key', 'population', 'vintage']], 'fact_population_annual')
    return (counties, pop)

def clean_weather_elements(raw, start_date=None, end_date=None):
    """Check weather flags and units, then pivot PRCP/TMIN/TMAX into one station-day row. Quarantine conflicting temperatures."""
    r = raw.copy()
    selected = r['date'].between((start_date or START).replace('-', ''), (end_date or END).replace('-', '')) & r['element'].isin(['PRCP', 'TMIN', 'TMAX'])
    r = r[selected].copy()
    stats = {'raw_weather_elements': len(r), 'weather_quality_flagged_elements': int(r['qflag'].str.strip().ne('').sum()), 'weather_presumed_zero_elements': int(r['mflag'].eq('P').sum())}
    if r.duplicated(['station_id', 'date', 'element']).any():
        raise ValueError('Duplicate station-date-element')
    n = number(r['value'])
    accepted = r['qflag'].str.strip().eq('') & r['mflag'].ne('P') & n.notna() & n.ne(-9999)
    r = r[accepted].copy()
    r['measure'] = n[accepted] / 10
    negative = r['element'].eq('PRCP') & r['measure'].lt(0)
    r.loc[negative, 'measure'] = np.nan
    wide = r.pivot(index='date', columns='element', values='measure').reindex(columns=['PRCP', 'TMIN', 'TMAX']).rename(columns={'PRCP': 'prcp_mm', 'TMIN': 'tmin_c', 'TMAX': 'tmax_c'})
    trace = r[r['element'].eq('PRCP')].set_index('date')['mflag'].eq('T')
    wide['trace_precipitation'] = trace.reindex(wide.index, fill_value=False)
    bad = wide['tmin_c'].gt(wide['tmax_c'])
    rejects = wide.loc[bad, ['tmin_c', 'tmax_c']].reset_index()
    rejects['reason'] = 'TMIN exceeds TMAX'
    wide.loc[bad, ['tmin_c', 'tmax_c']] = np.nan
    wide = wide.reset_index()
    wide['date_key'] = wide['date'].astype('int32')
    return (wide, stats, rejects)

def clean_weather():
    """Read selected station metadata and compressed files. Prepare clean station and daily weather tables."""
    selected = sorted(json.loads((ROOT / 'Original_Data/selected_stations.json').read_text()))
    records = []
    with open(CFG['station_metadata_path'], encoding='utf-8') as f:
        for line in f:
            if line[:11] in selected:
                records.append((line[:11], line[41:71].strip(), float(line[12:20]), float(line[21:30]), float(line[31:37])))
    stations = pd.DataFrame(records, columns=['station_id', 'station_name', 'latitude', 'longitude', 'elevation_m']).sort_values('station_id').reset_index(drop=True)
    assert len(stations) == len(selected) and (not stations.station_id.duplicated().any())
    assert stations.latitude.between(-90, 90).all() and stations.longitude.between(-180, 180).all()
    stations['elevation_m'] = stations.elevation_m.mask(stations.elevation_m.eq(-999.9))
    stations.insert(0, 'station_key', np.arange(1, len(stations) + 1))
    save(stations, 'dim_station')
    keys = dict(zip(stations.station_id, stations.station_key))
    pieces = []
    rejected = []
    names = ['station_id', 'date', 'element', 'value', 'mflag', 'qflag', 'sflag', 'obs_time']
    for i, path in enumerate(CFG['weather_files'], 1):
        chunks = []
        for raw in pd.read_csv(path, header=None, names=names, dtype=str, keep_default_na=False, chunksize=200000, compression='gzip'):
            mask = raw.date.between(START.replace('-', ''), END.replace('-', '')) & raw.element.isin(['PRCP', 'TMIN', 'TMAX'])
            chunks.append(raw[mask])
        raw = pd.concat(chunks, ignore_index=True)
        sid = Path(path).name.split('.')[0]
        if not raw.empty and (not raw.station_id.eq(sid).all()):
            raise ValueError('Wrong station file')
        wide, stats, bad = clean_weather_elements(raw)
        for name, value in stats.items():
            METRICS[name] = METRICS.get(name, 0) + value
        wide['station_key'] = keys[sid]
        wide['source_key'] = 2
        pieces.append(wide[['station_key', 'date_key', 'source_key', 'prcp_mm', 'tmin_c', 'tmax_c', 'trace_precipitation']])
        if len(bad):
            bad['station_id'] = sid
            rejected.append(bad)
        if i % 20 == 0 or i == len(CFG['weather_files']):
            log(f"Weather: {i}/{len(CFG['weather_files'])} station files cleaned")
    weather = pd.concat(pieces, ignore_index=True)
    assert not weather.duplicated(['station_key', 'date_key']).any()
    save(weather, 'fact_weather_daily')
    save(pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame(columns=['date', 'tmin_c', 'tmax_c', 'reason', 'station_id']), 'weather_rejects')
    return (stations, weather)

def xyz(lat, lon):
    """Convert latitude/longitude into three-dimensional points on a sphere for efficient distance searches."""
    lat = np.radians(np.asarray(lat, dtype=float))
    lon = np.radians(np.asarray(lon, dtype=float))
    return EARTH_RADIUS * np.column_stack((np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat)))

class WeatherMatcher:

    def __init__(self, stations, weather, max_distance_m=50000):
        """Build one search tree per date using stations with all three accepted weather measurements."""
        self.cache = {}
        self.max_distance = max_distance_m
        self.chord_limit = 2 * EARTH_RADIUS * np.sin(max_distance_m / (2 * EARTH_RADIUS))
        eligible = weather.dropna(subset=['prcp_mm', 'tmin_c', 'tmax_c']).merge(stations[['station_key', 'station_id', 'latitude', 'longitude']], on='station_key', validate='many_to_one')
        for day, g in eligible.groupby('date_key', sort=False):
            g = g.sort_values('station_id')
            points = xyz(g.latitude, g.longitude)
            values = g[['station_key', 'prcp_mm', 'tmin_c', 'tmax_c']].to_numpy(dtype=float)
            self.cache[int(day)] = (cKDTree(points), values, g.station_id.to_numpy())

    def match(self, c):
        """Choose the nearest complete same-date weather record within 50 km. Preserve missing matches and resolve ties by station ID."""
        result = np.full((len(c), 5), np.nan)
        good = np.flatnonzero(c['geocoded'].to_numpy(dtype=bool))
        days = c['date_key'].to_numpy(dtype=np.int64)
        for day in np.unique(days[good]):
            pos = good[days[good] == day]
            if int(day) not in self.cache:
                continue
            tree, values, ids = self.cache[int(day)]
            points = xyz(c.iloc[pos].latitude, c.iloc[pos].longitude)
            distances, index = tree.query(points, k=2, distance_upper_bound=self.chord_limit + 1e-08)
            nearest = distances[:, 0]
            pick = index[:, 0].copy()
            ok = np.isfinite(nearest)
            tied = np.flatnonzero(ok & np.isfinite(distances[:, 1]) & np.isclose(nearest, distances[:, 1], rtol=0, atol=1e-07))
            for j in tied:
                candidates = tree.query_ball_point(points[j], nearest[j] + 1e-07)
                exact = np.linalg.norm(tree.data[candidates] - points[j], axis=1)
                equal = np.array(candidates)[np.isclose(exact, exact.min(), rtol=0, atol=1e-07)]
                pick[j] = min(equal, key=lambda v: ids[v])
                nearest[j] = np.linalg.norm(tree.data[pick[j]] - points[j])
            arc = 2 * EARTH_RADIUS * np.arcsin(np.clip(nearest / (2 * EARTH_RADIUS), 0, 1))
            ok = ok & (arc <= self.max_distance + 1e-07)
            result[pos[ok], :4] = values[pick[ok]]
            result[pos[ok], 4] = np.minimum(arc[ok], self.max_distance)
        return pd.DataFrame(result, columns=['station_key', 'prcp_mm', 'tmin_c', 'tmax_c', 'station_distance_m'], index=c.index).assign(station_key=lambda x: x.station_key.astype('Int64'))

def dimensions():
    """Create the calendar, hour, year, and source reference tables used by the facts."""
    dates = pd.DataFrame({'full_date': pd.date_range(START, END[:4] + '-12-31')})
    dates['date_key'] = dates.full_date.dt.strftime('%Y%m%d').astype(int)
    for key in ['year', 'month', 'quarter']:
        dates[key] = getattr(dates.full_date.dt, key)
    dates['iso_weekday'] = dates.full_date.dt.dayofweek + 1
    dates['is_weekend'] = dates.iso_weekday > 5
    dates.full_date = dates.full_date.dt.strftime('%Y-%m-%d')
    save(dates[['date_key', 'full_date', 'year', 'month', 'quarter', 'iso_weekday', 'is_weekend']], 'dim_date')
    hours = pd.DataFrame({'time_key': range(24), 'hour': range(24)})
    hours['time_band'] = np.select([hours.hour < 6, hours.hour < 12, hours.hour < 18], ['overnight', 'morning', 'afternoon'], default='evening')
    hours = pd.concat([pd.DataFrame([[-1, pd.NA, 'unknown']], columns=hours.columns), hours], ignore_index=True)
    hours.hour = hours.hour.astype('Int64')
    save(hours, 'dim_time')
    save(pd.DataFrame({'year_key': range(int(START[:4]), int(END[:4]) + 1)}), 'dim_year')
    save(pd.DataFrame([[1, 'SWITRS', CFG['traffic_release']], [2, 'NOAA GHCN-Daily', 'Frozen station files; see source manifest'], [3, 'U.S. Census PEP', 'Intercensal + vintage 2020 + vintage 2021']], columns=['source_key', 'source_name', 'release']), 'dim_source')

def verify_sources():
    """Compare original files with recorded SHA-256 checksums. Stop if a frozen source has changed."""
    manifest = ROOT / 'Support/Generated/Checks/current_manifest.json'
    old = json.loads(manifest.read_text(encoding='utf-8')) if manifest.exists() else {}
    files = {str(Path(r['file']).resolve()): r['sha256'] for r in old.get('files', [])}
    catalog = ROOT / 'Support/Generated/Checks/Source_Catalog.csv'
    if catalog.exists():
        with catalog.open(encoding='utf-8-sig', newline='') as f:
            for r in csv.DictReader(f):
                files[str((ROOT / r['file']).resolve())] = r['sha256']
    inputs = [CFG['sqlite_path'], CFG['station_metadata_path'], CFG['county_crosswalk_path'], str(ROOT/'Original_Data/selected_stations.json')] + CFG['weather_files'] + [r['path'] for r in CFG['population_files']]
    for name in inputs:
        files.setdefault(str(Path(name).resolve()), None)
    verified = []
    for name, expected in files.items():
        h = hashlib.sha256()
        with open(name, 'rb') as f:
            for block in iter(lambda: f.read(8 * 1024 * 1024), b''):
                h.update(block)
        if expected is None:
            expected = h.hexdigest()
        elif h.hexdigest() != expected:
            if Path(name).resolve() == Path(CFG['sqlite_path']).resolve() and ALLOW_TRAFFIC_UPDATES:
                global TRAFFIC_CHANGED
                TRAFFIC_CHANGED = True
                expected = h.hexdigest()
                log('Traffic SQLite changed; a fresh source profile is required for this full rebuild.')
            else:
                raise ValueError('Original source changed: ' + name)
        verified.append({'file': name, 'sha256': expected, 'bytes': Path(name).stat().st_size})
    (ROOT / 'Support/Generated/Checks/python_source_manifest.json').write_text(json.dumps(verified, indent=2))
    log(f'Checked {len(verified)} source files; traffic update recorded' if TRAFFIC_CHANGED else f'Checked {len(verified)} source files; all hashes unchanged')
    return verified

def prepare(limit=None):
    """Run the preparation in order: verify sources, prepare dimensions/population, clean weather, process collision chunks, and calculate marts."""
    global END
    with closing(sqlite3.connect(Path(CFG['sqlite_path']).resolve().as_uri() + '?mode=ro', uri=True)) as source:
        maximum = source.execute('SELECT max(collision_date) FROM collisions').fetchone()[0]
        year_bounds = source.execute("SELECT substr(collision_date,1,4),min(collision_date),max(collision_date) FROM collisions GROUP BY substr(collision_date,1,4)").fetchall()
    complete_years = {int(year) for year,first,last in year_bounds if str(first)[5:10]=='01-01' and str(last)[5:10]=='12-31'}
    END = str(maximum)[:10]
    CFG['traffic_release'] = f'local SWITRS SQLite; {START} through {END}'
    METRICS.clear()
    TIMINGS.clear()
    start = time.time()
    batch = str(uuid.uuid4())
    # STEP 1: check fingerprints of the original files.
    verified = verify_sources()
    if TRAFFIC_CHANGED or not (ROOT/'Support/Generated/Checks/source_profile.json').exists():
        import download_sources
        download_sources.profile_traffic_source(ROOT)
    phase = time.time()
    # STEP 2: create calendar keys and prepare county population.
    dimensions()
    counties, pop = clean_population()
    TIMINGS['population_and_dimensions'] = time.time() - phase
    phase = time.time()
    # STEP 3: clean weather and build same-date station searches.
    stations, weather = clean_weather()
    TIMINGS['weather_cleaning'] = time.time() - phase
    phase = time.time()
    matcher = WeatherMatcher(stations, weather)
    TIMINGS['weather_search_preparation'] = time.time() - phase
    lookup = pop[['county_key', 'year_key', 'population', 'vintage']].rename(columns={'year_key': 'year'})
    county_names = dict(zip(counties.county_key, counties.county_name))
    fipsmap = dict(zip(counties.fips, counties.county_key))
    cross = pd.read_csv(CFG['county_crosswalk_path'], dtype=str, keep_default_na=False)
    assert not cross.source_county.duplicated().any() and cross.fips.isin(fipsmap).all()
    countykeys = {r.source_county: fipsmap[r.fips] for r in cross.itertuples()}
    fips_names = dict(zip(counties.county_key, counties.fips))
    contexts = {}
    aggregates = []
    coverage = []
    examples = []
    metrics = dict(raw_collisions=0, accepted_collisions=0, rejected_collisions=0, unknown_counties=0, unknown_times=0, geocoded=0, weather_matched=0, population_matched=0, killed_total=0, injured_total=0, unknown_severity_labels=0)
    renaming = {v: k for k, v in CFG['columns'].items()}
    projection = ','.join(('"' + s.replace('"', '""') + '"' for s in CFG['traffic_projection']))
    sql = 'SELECT ' + projection + ' FROM collisions' + (f' LIMIT {int(limit)}' if limit else '')
    uri = Path(CFG['sqlite_path']).resolve().as_uri() + '?mode=ro'
    phase = time.time()
    first = True
    first_reject = True
    with closing(sqlite3.connect(uri, uri=True)) as source:
        for chunk_no, raw in enumerate(pd.read_sql_query(sql, source, chunksize=100000), 1):
            c, rejects = clean_traffic(raw)
            metrics['raw_collisions'] += len(raw)
            metrics['rejected_collisions'] += len(rejects)
            if len(rejects):
                save(rejects, 'collision_rejects', not first_reject)
                first_reject = False
            if not len(c):
                continue
            c['county_key'] = category(c['county']).map(countykeys).fillna(0).astype('int32')
            combos = list(zip(c.lighting, c.road_surface, c.reported_weather))
            for combo in dict.fromkeys(combos):
                if combo not in contexts:
                    contexts[combo] = len(contexts) + 1
            c['context_key'] = [contexts[x] for x in combos]
            c['source_key'] = 1
            c['collision_count'] = 1
            c['batch_id'] = batch
            for name in ['primary_road', 'secondary_road', 'primary_collision_factor', 'type_of_collision']:
                c[name] = category(c[name])
            # STEP 4: c contains clean collisions; w contains their weather matches.
            c = c.reset_index(drop=True)
            w = matcher.match(c)
            c = pd.concat([c, w], axis=1).merge(lookup, on=['county_key', 'year'], how='left', validate='many_to_one', sort=False)
            c['population'] = c.population.astype('Int64')
            c['county_name'] = c.county_key.map(county_names)
            c['fips'] = c.county_key.map(fips_names)
            c['weather_matched'] = c.station_key.notna()
            c['population_matched'] = c.population.notna()
            c['hour'] = c.time_key.where(c.time_key.ne(-1)).astype('Int64')
            c['severe_or_fatal'] = c.severity.map({'fatal': 1, 'severe injury': 1, 'other injury': 0, 'pain': 0, 'property damage only': 0}).astype('Int64')
            save(c[FACT_COLS], 'fact_collision', not first)
            context_cols = FACT_COLS + ['full_date', 'year', 'month', 'iso_weekday', 'is_weekend', 'hour', 'fips', 'county_name', 'lighting', 'road_surface', 'reported_weather', 'population', 'vintage', 'station_key', 'prcp_mm', 'tmin_c', 'tmax_c', 'station_distance_m', 'weather_matched', 'population_matched', 'geocoded', 'severe_or_fatal']
            c['full_date'] = c.full_date.dt.strftime('%Y-%m-%d')
            save(c[context_cols], 'collision_context', not first)
            first = False
            metrics['unknown_severity_labels'] += int(c.severe_or_fatal.isna().sum())
            metrics['accepted_collisions'] += len(c)
            metrics['unknown_counties'] += int(c.county_key.eq(0).sum())
            metrics['unknown_times'] += int(c.time_key.eq(-1).sum())
            for name in ['geocoded', 'weather_matched', 'population_matched']:
                metrics[name] += int(c[name].sum())
            for name in ['killed', 'injured']:
                metrics[name + '_total'] += int(c[name].sum())
            a = c.groupby(['county_key', 'year'], sort=False).agg(collisions=('case_id', 'size'), severe_fatal_collisions=('severe_or_fatal', 'sum'), killed=('killed', lambda s: s.sum(min_count=1)), injured=('injured', lambda s: s.sum(min_count=1)), missing_killed_counts=('killed', lambda s: s.isna().sum()), missing_injured_counts=('injured', lambda s: s.isna().sum()), weather_matches=('weather_matched', 'sum')).reset_index()
            aggregates.append(a)
            coverage.append(c.groupby('year').agg(collision_count=('case_id', 'size'), weather_matches=('weather_matched', 'sum'), population_matches=('population_matched', 'sum'), coordinates=('geocoded', 'sum')).reset_index())
            if chunk_no == 1 or (len(examples) < 30 and c.geocoded.any()):
                picked = c.head(5) if chunk_no == 1 else c[c.geocoded].head(5)
                examples.extend(picked[['case_id', 'full_date', 'time_key', 'county_name', 'severity', 'killed', 'injured', 'latitude', 'longitude', 'population', 'station_key', 'station_distance_m', 'weather_matched', 'severe_or_fatal']].to_dict('records'))
            log(f"Python cleaned, matched and staged {metrics['accepted_collisions']:,} collisions")
    if first_reject:
        save(pd.DataFrame(columns=['case_id', 'reject_reason']), 'collision_rejects')
    assert not first, 'No accepted collisions'
    dims = pd.DataFrame([(*key, val) for key, val in contexts.items()], columns=['lighting', 'road_surface', 'reported_weather', 'context_key'])
    save(dims[['context_key', 'lighting', 'road_surface', 'reported_weather']], 'dim_context')
    save(pd.DataFrame(examples), 'Cleaning_Examples')
    # STEP 5: combine chunk totals into county-year reporting rows.
    all_agg = pd.concat(aggregates).groupby(['county_key', 'year'], as_index=False).sum(min_count=1)
    rates = lookup.merge(all_agg, on=['county_key', 'year'], how='outer', validate='one_to_one')
    rates['population'] = rates.population.astype('Int64')
    rates['fips'] = rates.county_key.map(fips_names)
    rates['county_name'] = rates.county_key.map(county_names)
    for k in ['collisions', 'severe_fatal_collisions']:
        rates[k] = rates[k].fillna(0).astype('int64')
    rates['weather_match_fraction'] = rates.weather_matches / rates.collisions.replace(0, np.nan)
    rates['partial_year'] = ~rates.year.isin(complete_years)
    rates['collisions_per_100k'] = (100000 * rates.collisions / rates.population).where(~rates.partial_year)
    ratecols = ['fips', 'county_name', 'year', 'collisions', 'severe_fatal_collisions', 'killed', 'injured', 'missing_killed_counts', 'missing_injured_counts', 'population', 'vintage', 'weather_match_fraction', 'partial_year', 'collisions_per_100k']
    for k in ['killed', 'injured', 'missing_killed_counts', 'missing_injured_counts']:
        rates[k] = rates[k].astype('Int64')
    save(rates[ratecols], 'safety_county_year')
    byyear = pd.concat(coverage).groupby('year', as_index=False).sum()
    for old, new in [('weather_matches', 'weather_coverage'), ('population_matches', 'population_coverage'), ('coordinates', 'coordinate_coverage')]:
        byyear[new] = byyear[old] / byyear.collision_count
    byyear[['year', 'collision_count', 'weather_coverage', 'population_coverage', 'coordinate_coverage']].to_csv(ROOT / 'Support/Generated/Checks/Coverage_By_Year.csv', index=False)
    TIMINGS['traffic_cleaning_matching_and_marts'] = time.time() - phase
    METRICS.update({k: v for k, v in metrics.items() if k not in ['geocoded', 'weather_matched', 'population_matched', 'killed_total', 'injured_total']})
    for k, new in [('geocoded', 'geocoded_fraction'), ('weather_matched', 'weather_match_fraction'), ('population_matched', 'population_match_fraction')]:
        METRICS[new] = metrics[k] / metrics['accepted_collisions']
    METRICS.update(population_rows=len(pop), weather_station_days=len(weather))
    summary = {'batch_id': batch, 'started_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'status': 'python_staged', 'pipeline': 'Python ETL', 'metrics': METRICS, 'totals': metrics, 'timings': TIMINGS, 'prepare_seconds': time.time() - start, 'limit': limit, 'config': public_config(), 'files': verified, 'station_count': len(stations), 'source_end_date': END, 'source_start_date': START, 'partial_years': sorted(rates.loc[rates.partial_year, 'year'].astype(int).unique().tolist())}
    (ROOT / 'Support/Generated/Checks/python_run.json').write_text(json.dumps(summary, indent=2))
    log('All Python cleaning, source joins and mart calculations completed')
    return summary

# SECTION 2: database loading, constraints, and publication.
"""Student guide: this file performs the Load part of ETL.
Python sends already-cleaned files to PostgreSQL; SQL provides storage and constraints.
Read load_and_publish() first to follow the complete loading sequence."""
from psycopg import sql
S = {
    'dw': 'python_dw',
    'mart': 'python_mart',
    'staging': 'python_staging',
    'audit': 'python_audit',
    'landing': 'python_landing',
}
DIMS = {
    'dim_date': {'date_key': 'int', 'full_date': 'date', 'year': 'int', 'month': 'int', 'quarter': 'int', 'iso_weekday': 'int', 'is_weekend': 'boolean'},
    'dim_time': {'time_key': 'int', 'hour': 'int', 'time_band': 'text'},
    'dim_year': {'year_key': 'int'},
    'dim_county': {'county_key': 'int', 'fips': 'text', 'county_name': 'text', 'state_name': 'text'},
    'dim_context': {'context_key': 'int', 'lighting': 'text', 'road_surface': 'text', 'reported_weather': 'text'},
    'dim_station': {'station_key': 'int', 'station_id': 'text', 'station_name': 'text', 'latitude': 'double precision', 'longitude': 'double precision', 'elevation_m': 'double precision'},
    'dim_source': {'source_key': 'int', 'source_name': 'text', 'release': 'text'},
}
COLLISION = {k: 'text' for k in FACT_COLS}
for k in ['date_key', 'time_key', 'county_key', 'context_key', 'source_key', 'collision_count', 'minute_of_day']:
    COLLISION[k] = 'int'
for k in ['killed', 'injured']:
    COLLISION[k] = 'bigint'
for k in ['latitude', 'longitude']:
    COLLISION[k] = 'double precision'
WEATHER = {
    'station_key': 'int',
    'date_key': 'int',
    'source_key': 'int',
    'prcp_mm': 'double precision',
    'tmin_c': 'double precision',
    'tmax_c': 'double precision',
    'trace_precipitation': 'boolean',
}
POP = {
    'county_key': 'int',
    'year_key': 'int',
    'source_key': 'int',
    'population': 'bigint',
    'vintage': 'text',
}
CONTEXT = {
    **COLLISION,
    'full_date': 'date',
    'year': 'int',
    'month': 'int',
    'iso_weekday': 'int',
    'is_weekend': 'boolean',
    'hour': 'int',
    'fips': 'text',
    'county_name': 'text',
    'lighting': 'text',
    'road_surface': 'text',
    'reported_weather': 'text',
    'population': 'bigint',
    'vintage': 'text',
    'station_key': 'int',
    'prcp_mm': 'double precision',
    'tmin_c': 'double precision',
    'tmax_c': 'double precision',
    'station_distance_m': 'double precision',
    'weather_matched': 'boolean',
    'population_matched': 'boolean',
    'geocoded': 'boolean',
    'severe_or_fatal': 'smallint',
}
RATES = {
    'fips': 'text',
    'county_name': 'text',
    'year': 'int',
    'collisions': 'bigint',
    'severe_fatal_collisions': 'bigint',
    'killed': 'bigint',
    'injured': 'bigint',
    'missing_killed_counts': 'bigint',
    'missing_injured_counts': 'bigint',
    'population': 'bigint',
    'vintage': 'text',
    'weather_match_fraction': 'double precision',
    'partial_year': 'boolean',
    'collisions_per_100k': 'double precision',
}

def table_and_copy(db, schema, table, columns, filename):
    """Create a storage table, check its CSV column order, and stream the already-cleaned file into PostgreSQL."""
    definition = sql.SQL(',').join((sql.SQL('{} {}').format(sql.Identifier(k), sql.SQL(v)) for k, v in columns.items()))
    db.execute(sql.SQL('CREATE TABLE {}.{} ({})').format(sql.Identifier(schema), sql.Identifier(table), definition))
    with open(OUT / (filename + '.csv'), 'r', encoding='utf-8', newline='') as f:
        actual = next(csv.reader(f))
    if actual != list(columns):
        raise ValueError('CSV columns disagree with storage schema: ' + filename)
    command = sql.SQL("COPY {}.{} FROM STDIN WITH (FORMAT CSV,HEADER TRUE,NULL '')").format(sql.Identifier(schema), sql.Identifier(table))
    with db.cursor().copy(command) as cp:
        with open(OUT / (filename + '.csv'), 'rb') as f:
            for block in iter(lambda: f.read(1024 * 1024), b''):
                cp.write(block)
    log(f'Loaded {schema}.{table} from the Python-cleaned CSV')

def keys(db):
    """Add primary keys, unique rules, value checks, and foreign keys. Validate all links before publication."""
    for name, cols in DIMS.items():
        pk = next(iter(cols))
        db.execute(f'ALTER TABLE python_dw.{name} ADD PRIMARY KEY({pk})')
    db.execute('ALTER TABLE python_dw.dim_county ADD UNIQUE(fips)')
    db.execute('ALTER TABLE python_dw.dim_station ADD UNIQUE(station_id)')
    db.execute('ALTER TABLE python_dw.dim_context ADD UNIQUE(lighting,road_surface,reported_weather)')
    for table, pk in [('fact_collision', 'case_id'), ('fact_weather_daily', 'station_key,date_key'), ('fact_population_annual', 'county_key,year_key')]:
        db.execute(f'ALTER TABLE python_dw.{table} ADD PRIMARY KEY({pk})')
    for table, pk in [('collision_context', 'case_id'), ('safety_county_year', 'fips,year')]:
        db.execute(f'ALTER TABLE python_mart.{table} ADD PRIMARY KEY({pk})')
    checks = {'fact_collision': 'CHECK(collision_count=1 AND (killed IS NULL OR killed>=0) AND (injured IS NULL OR injured>=0))', 'fact_weather_daily': 'CHECK((prcp_mm IS NULL OR prcp_mm>=0) AND (tmin_c IS NULL OR tmax_c IS NULL OR tmin_c<=tmax_c))', 'fact_population_annual': 'CHECK(population>0)'}
    for table, check in checks.items():
        db.execute(f'ALTER TABLE python_dw.{table} ADD {check}')
    references = {'fact_collision': ['date', 'time', 'county', 'context', 'source'], 'fact_weather_daily': ['date', 'station', 'source'], 'fact_population_annual': ['county', 'year', 'source']}
    for table, dims in references.items():
        for dim in dims:
            name = f'{table}_{dim}_fk'
            db.execute(f'ALTER TABLE python_dw.{table} ADD CONSTRAINT {name} FOREIGN KEY({dim}_key) REFERENCES python_dw.dim_{dim}({dim}_key) NOT VALID')
            db.execute(f'ALTER TABLE python_dw.{table} VALIDATE CONSTRAINT {name}')
    db.execute('CREATE INDEX ON python_dw.fact_collision(date_key,county_key)')
    db.execute('CREATE INDEX ON python_dw.fact_weather_daily(date_key)')
    db.execute('CREATE INDEX ON python_mart.collision_context(year,county_key)')
    log('Primary keys, uniqueness, checks and all fact foreign keys validated')

def simple_views(db):
    """Expose selected columns and rows for analysis. The cleaning, source joins, targets, and rates were already calculated in Python."""
    db.execute('CREATE VIEW python_mart.severity_retrospective AS SELECT * FROM python_mart.collision_context')
    db.execute('CREATE VIEW python_mart.severity_predictors AS SELECT case_id,full_date,year,month,iso_weekday,is_weekend,hour,county_name,lighting,road_surface,reported_weather,population,population_matched FROM python_mart.collision_context')
    db.execute('CREATE VIEW python_mart.severity_labels AS SELECT case_id,severity,severe_or_fatal FROM python_mart.collision_context')
    db.execute('CREATE VIEW python_mart.spatial_clustering AS SELECT case_id,full_date,hour,latitude,longitude,weather_matched,station_distance_m FROM python_mart.collision_context WHERE geocoded')
    db.execute('CREATE VIEW python_landing.files AS SELECT * FROM python_audit.files')

def refresh_snowflake(db, schemas):
    """Build small normalized dimensions in Python and expose one combined view.
    Date points to Month and then Year. County points to State.
    Large collision, weather, and population facts are reused, not copied.
    Run inside the caller's transaction after full loading or date updates.
    """
    dw, mart = schemas['dw'], schemas['mart']
    definitions = {
        'sf_dim_state': 'state_key int PRIMARY KEY, state_name text NOT NULL',
        'sf_dim_year': 'year_key int PRIMARY KEY',
        'sf_dim_month': f'month_key int PRIMARY KEY, month int NOT NULL, quarter int NOT NULL, year_key int REFERENCES {mart}.sf_dim_year(year_key)',
        'sf_dim_date': f'date_key int PRIMARY KEY, full_date date NOT NULL, iso_weekday int, is_weekend boolean, month_key int REFERENCES {mart}.sf_dim_month(month_key)',
        'sf_dim_county': f'county_key int PRIMARY KEY, fips text, county_name text, state_key int REFERENCES {mart}.sf_dim_state(state_key)',
    }
    for name, definition in definitions.items():
        db.execute(f'CREATE TABLE IF NOT EXISTS {mart}.{name} ({definition})')
    dates = pd.DataFrame(db.execute(f'SELECT date_key,full_date,year,month,quarter,iso_weekday,is_weekend FROM {dw}.dim_date').fetchall(), columns=DIMS['dim_date'])
    counties = pd.DataFrame(db.execute(f'SELECT county_key,fips,county_name,state_name FROM {dw}.dim_county').fetchall(), columns=DIMS['dim_county'])
    # One California row is sufficient; key zero preserves an unknown county.
    states = pd.DataFrame({'state_key':[0,6], 'state_name':['Unknown','California']})
    counties['state_key'] = np.where(counties.county_key.eq(0),0,6)
    dates['month_key'] = dates.year * 100 + dates.month
    frames = {
        'sf_dim_state': states,
        'sf_dim_year': dates[['year']].rename(columns={'year':'year_key'}).drop_duplicates(),
        'sf_dim_month': dates[['month_key','month','quarter','year']].rename(columns={'year':'year_key'}).drop_duplicates(),
        'sf_dim_date': dates[['date_key','full_date','iso_weekday','is_weekend','month_key']],
        'sf_dim_county': counties[['county_key','fips','county_name','state_key']],
    }
    for name, frame in frames.items():
        upsert_frame(db, mart, name, frame, [frame.columns[0]])
    # The existing Python match supplies the station key and match distance.
    # It does not rematch weather in SQL or join weather by county alone.
    db.execute(f"""CREATE OR REPLACE VIEW {mart}.sf_fact_collision AS
        SELECT f.*, m.station_key, m.station_distance_m,
               m.weather_matched, m.population_matched, m.geocoded, m.severe_or_fatal
        FROM {dw}.fact_collision f
        LEFT JOIN {mart}.collision_context m USING(case_id)""")
    db.execute(f"""CREATE OR REPLACE VIEW {mart}.collision_snowflake AS
        SELECT f.*, d.full_date, y.year_key AS year, mo.month, mo.quarter,
               d.iso_weekday, d.is_weekend, t.hour, t.time_band,
               c.fips, c.county_name, st.state_name,
               x.lighting, x.road_surface, x.reported_weather,
               sn.station_id, sn.station_name,
               sn.latitude AS station_latitude, sn.longitude AS station_longitude,
               w.prcp_mm, w.tmin_c, w.tmax_c, w.trace_precipitation,
               w.source_key AS weather_source_key,
               p.population, p.vintage, p.source_key AS population_source_key
        FROM {mart}.sf_fact_collision f
        LEFT JOIN {mart}.sf_dim_date d USING(date_key)
        LEFT JOIN {mart}.sf_dim_month mo USING(month_key)
        LEFT JOIN {mart}.sf_dim_year y ON y.year_key=mo.year_key
        LEFT JOIN {mart}.sf_dim_county c USING(county_key)
        LEFT JOIN {mart}.sf_dim_state st USING(state_key)
        LEFT JOIN {dw}.dim_time t USING(time_key)
        LEFT JOIN {dw}.dim_context x USING(context_key)
        LEFT JOIN {dw}.dim_station sn USING(station_key)
        LEFT JOIN {dw}.fact_weather_daily w ON w.station_key=f.station_key AND w.date_key=f.date_key
        LEFT JOIN {dw}.fact_population_annual p ON p.county_key=f.county_key AND p.year_key=y.year_key""")
    log('Snowflake dimensions refreshed; mart.collision_snowflake combines the three sources')


def validate(db, run):
    """Check complete source totals, loaded grains, population coverage, weather limits, and agreement with the saved verified reference."""
    profile = json.loads((ROOT / 'Support/Generated/Checks/source_profile.json').read_text())
    baseline = {'collisions': profile['counts']['collisions'], 'killed': sum((x['killed'] or 0 for x in profile['collisions'])), 'injured': sum((x['injured'] or 0 for x in profile['collisions']))}
    total = db.execute('SELECT count(*),sum(killed),sum(injured) FROM python_dw.fact_collision').fetchone()
    totals = dict(zip(['collisions', 'killed', 'injured'], map(int, total)))
    integrated = db.execute('SELECT count(*) FROM python_mart.collision_context').fetchone()[0]
    populations = [{'year_key': year, 'counties': n} for year, n in db.execute('SELECT year_key,count(*) FROM python_dw.fact_population_annual GROUP BY year_key ORDER BY year_key')]
    m = run['metrics']
    assertions = {'source_vs_collision_rows': totals['collisions'] == baseline['collisions'], 'accepted_plus_rejected': m['accepted_collisions'] + m['rejected_collisions'] == baseline['collisions'], 'integrated_one_row_per_case': integrated == totals['collisions'], 'killed_total_reconciled': totals['killed'] == baseline['killed'], 'injured_total_reconciled': totals['injured'] == baseline['injured'], 'loaded_weather_rows_match_python': db.execute('SELECT count(*) FROM python_dw.fact_weather_daily').fetchone()[0] == m['weather_station_days'], 'loaded_population_rows_match_python': db.execute('SELECT count(*) FROM python_dw.fact_population_annual').fetchone()[0] == m['population_rows'], 'loaded_weather_match_count': db.execute('SELECT count(*) FROM python_mart.collision_context WHERE weather_matched').fetchone()[0] == run['totals']['weather_matched'], 'loaded_coordinate_count': db.execute('SELECT count(*) FROM python_mart.collision_context WHERE geocoded').fetchone()[0] == run['totals']['geocoded'], 'population_58_counties_per_selected_year': len(populations) == sum(source['end_year']-source['start_year']+1 for source in CFG['population_files']) and all((x['counties'] == 58 for x in populations)), 'unknown_counties_zero': m['unknown_counties'] == 0, 'matched_weather_max_50km': db.execute('SELECT count(*) FROM python_mart.collision_context WHERE station_distance_m>50000').fetchone()[0] == 0, 'temperature_order': db.execute('SELECT count(*) FROM python_dw.fact_weather_daily WHERE tmin_c>tmax_c').fetchone()[0] == 0, 'partial_year_rates_hidden': db.execute('SELECT count(*) FROM python_mart.safety_county_year WHERE year=ANY(%s) AND collisions_per_100k IS NOT NULL', (run.get('partial_years',[2021]),)).fetchone()[0] == 0, 'unknown_severity_labels_match_python': db.execute('SELECT count(*) FROM python_mart.severity_labels WHERE severe_or_fatal IS NULL').fetchone()[0] == run['totals'].get('unknown_severity_labels', sum(count for severity,count in profile['severity'] if severity not in ['fatal','severe injury','other injury','pain','property damage only'])), 'all_fact_constraints_validated': db.execute("SELECT bool_and(convalidated) FROM pg_constraint WHERE conrelid IN ('python_dw.fact_collision'::regclass,'python_dw.fact_weather_daily'::regclass,'python_dw.fact_population_annual'::regclass)").fetchone()[0]}
    assertions['snowflake_dates_complete'] = db.execute('SELECT count(*) FROM python_mart.sf_dim_date').fetchone()[0] == db.execute('SELECT count(*) FROM python_dw.dim_date').fetchone()[0]
    assertions['snowflake_counties_complete'] = db.execute('SELECT count(*) FROM python_mart.sf_dim_county').fetchone()[0] == db.execute('SELECT count(*) FROM python_dw.dim_county').fetchone()[0]
    assertions['snowflake_links_valid'] = db.execute("SELECT bool_and(convalidated) FROM pg_constraint WHERE connamespace='python_mart'::regnamespace AND conrelid::regclass::text LIKE 'python_mart.sf_dim_%'").fetchone()[0]
    reference_path = ROOT / 'Support/Generated/Checks/reference_SQL_validation.json'
    old = json.loads(reference_path.read_text(encoding='utf-8')) if reference_path.exists() else {}
    # The SQL reference belongs to the original traffic file, even after later full runs.
    traffic_hash = next((file['sha256'] for file in run['files'] if Path(file['file']).resolve() == Path(CFG['sqlite_path']).resolve()), None)
    compare_reference = bool(old) and CFG.get('historical_reference_enabled',True) and not TRAFFIC_CHANGED and not run.get('sources_updated',False) and traffic_hash == CFG.get('reference_traffic_sha256')
    if not compare_reference:
        log('Historical traffic regression comparisons skipped: this is a different traffic release.')
    for key in ['accepted_collisions', 'population_rows', 'weather_station_days', 'unknown_counties', 'unknown_times', 'raw_weather_elements', 'weather_quality_flagged_elements', 'weather_presumed_zero_elements']:
        if compare_reference:
            assertions['previous_run_' + key] = m[key] == old['metrics'][key]
    for key in ['weather_match_fraction', 'population_match_fraction', 'geocoded_fraction']:
        if compare_reference:
            assertions['previous_run_' + key] = abs(m[key] - old['metrics'][key]) < 1e-12
    sample = [] if not compare_reference else db.execute('SELECT a.case_id FROM mart.collision_context a WHERE a.weather_matched LIMIT 500').fetchall()
    if sample:
        comparison = db.execute('SELECT count(*) FROM mart.collision_context a JOIN python_mart.collision_context b USING(case_id) LEFT JOIN dw.dim_station s ON a.station_key=s.station_key LEFT JOIN python_dw.dim_station t ON b.station_key=t.station_key WHERE a.case_id=ANY(%s) AND (s.station_id IS DISTINCT FROM t.station_id OR a.prcp_mm IS DISTINCT FROM b.prcp_mm OR a.tmin_c IS DISTINCT FROM b.tmin_c OR a.tmax_c IS DISTINCT FROM b.tmax_c OR abs(a.station_distance_m-b.station_distance_m)>0.001)', ([x[0] for x in sample],)).fetchone()[0]
        assertions['sample_500_weather_values_stations_distances_match'] = comparison == 0
    result = {'source_end_date': run.get('source_end_date', END), 'source_start_date': run.get('source_start_date', START), 'partial_years': run.get('partial_years',[2021]), 'checked_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'batch_id': run['batch_id'], 'pipeline': 'Python ETL', 'metrics': m, 'source_totals': baseline, 'warehouse_totals': totals, 'integrated_rows': integrated, 'station_count': run['station_count'], 'assertions': assertions, 'all_checks_passed': all(assertions.values()), 'database_size': 'pending publication', 'transform_seconds': run['prepare_seconds'], 'population_by_year': populations, 'coverage_by_year': pd.read_csv(ROOT / 'Support/Generated/Checks/Coverage_By_Year.csv').to_dict('records')}
    log('Validation checks: ' + json.dumps(assertions, sort_keys=True))
    if not result['all_checks_passed']:
        raise RuntimeError('Validation failed; existing canonical warehouse retained. Inspect Support/ETL_Run.log')
    log(f'All {len(assertions)} warehouse checks passed')
    return result

def load_and_publish():
    """Build replacement schemas, load prepared files, and publish only after validation. A transaction protects the existing warehouse on failure."""
    start = time.time()
    run = json.loads((ROOT / 'Support/Generated/Checks/python_run.json').read_text())
    if run.get('limit'):
        raise ValueError('A sample must never replace the full warehouse')
    with connect() as db:
        db.execute("SET application_name='Python ETL warehouse loader'")
        db.execute("SET maintenance_work_mem='256MB'")
        # A failure rolls back the replacement and preserves the current warehouse.
        with db.transaction():
            db.execute('SELECT pg_advisory_xact_lock(5442026)')
            for schema in S.values():
                db.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
                db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            for name, columns in DIMS.items():
                table_and_copy(db, S['dw'], name, columns, name)
            for name, cols in [('fact_population_annual', POP), ('fact_weather_daily', WEATHER), ('fact_collision', COLLISION)]:
                table_and_copy(db, S['dw'], name, cols, name)
                table_and_copy(db, S['staging'], name.removeprefix('fact_'), cols, name)
            table_and_copy(db, S['staging'], 'station', DIMS['dim_station'], 'dim_station')
            table_and_copy(db, S['mart'], 'collision_context', CONTEXT, 'collision_context')
            table_and_copy(db, S['mart'], 'safety_county_year', RATES, 'safety_county_year')
            keys(db)
            db.execute('CREATE TABLE python_audit.batches(batch_id text PRIMARY KEY,started_at timestamptz,status text,details jsonb)')
            db.execute('INSERT INTO python_audit.batches VALUES(%s,now(),%s,%s)', (run['batch_id'], 'validated_replacement', json.dumps({'pipeline': 'Python ETL', 'source_manifest': 'Support/Generated/Checks/python_source_manifest.json'})))
            db.execute('CREATE TABLE python_audit.files(batch_id text,file_id text,path text,source text,sha256 text,row_count bigint)')
            for f in run['files']:
                source = 'SWITRS' if f['file'].endswith('.sqlite') else 'NOAA' if 'ghcnd-' in f['file'] or f['file'].endswith('.csv.gz') else 'Census' if 'co-est' in f['file'] else 'mapping_or_selection'
                db.execute('INSERT INTO python_audit.files VALUES(%s,%s,%s,%s,%s,%s)', (run['batch_id'], f['sha256'], f['file'], source, f['sha256'], None))
            db.execute('CREATE TABLE python_audit.quality(batch_id text,metric text,value numeric,created_at timestamptz DEFAULT now())')
            for name, value in run['metrics'].items():
                db.execute('INSERT INTO python_audit.quality(batch_id,metric,value) VALUES(%s,%s,%s)', (run['batch_id'], name, value))
            db.execute('CREATE TABLE python_audit.reject_files(kind text,path text)')
            for kind in ['collision_rejects', 'weather_rejects']:
                db.execute('INSERT INTO python_audit.reject_files VALUES(%s,%s)', (kind, str(OUT / (kind + '.csv'))))
            simple_views(db)
            refresh_snowflake(db, S)
            for schema in [S['dw'], S['mart']]:
                for name, in db.execute('SELECT tablename FROM pg_tables WHERE schemaname=%s', (schema,)).fetchall():
                    db.execute(sql.SQL('ANALYZE {}.{}').format(sql.Identifier(schema), sql.Identifier(name)))
            result = validate(db, run)
            for canonical in ['mart', 'dw', 'staging', 'landing', 'audit']:
                db.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(canonical)))
            for canonical, replacement in S.items():
                db.execute(sql.SQL('ALTER SCHEMA {} RENAME TO {}').format(sql.Identifier(replacement), sql.Identifier(canonical)))
            db.execute('UPDATE audit.batches SET status=%s WHERE batch_id=%s', ('success', run['batch_id']))
        run['load_seconds'] = time.time() - start
        run['transform_seconds'] = run['prepare_seconds'] + run['load_seconds']
        run['status'] = 'success'
        result['transform_seconds'] = run['transform_seconds']
        result['database_size'] = db.execute('SELECT pg_size_pretty(pg_database_size(current_database()))').fetchone()[0]
    (ROOT / 'Support/Generated/Checks/python_run.json').write_text(json.dumps(run, indent=2))
    (ROOT / 'Support/Generated/Checks/current_manifest.json').write_text(json.dumps(run, indent=2))
    (ROOT / 'Support/Generated/Checks/warehouse_validation.json').write_text(json.dumps(result, indent=2))
    log(f"Published rebuilt Python warehouse. Total run: {run['transform_seconds']:.1f} seconds")

# SECTION 3: small tests of the same cleaning functions.
"""Student guide: small examples check the important cleaning and matching rules without reloading PostgreSQL."""
import unittest
import pandas as pd
import numpy as np

class Rules(unittest.TestCase):

    def test_unknown_and_overflow_numbers(self):
        n = number(pd.Series(['12', '-3', 'bad', 'NaN', '1e309', None]))
        self.assertEqual(n.iloc[0], 12)
        self.assertTrue(n.iloc[2:].isna().all())
        c = nonnegative_count(pd.Series(['0', '2', '-1', '1.5', None]))
        self.assertEqual(c.tolist()[:2], [0, 2])
        self.assertTrue(c.iloc[2:].isna().all())

    def test_time_unknown_and_midnight(self):
        h, m = clean_time(pd.Series(['0000', '12:45:00', '9999', '2460', None]))
        self.assertEqual(h.tolist(), [0, 12, -1, -1, -1])
        self.assertEqual(m.iloc[1], 765)

    def test_flags_units_and_bad_temperature(self):
        rows = [['S', '20200101', 'PRCP', '12', 'T', ''], ['S', '20200101', 'TMIN', '50', '', ''], ['S', '20200101', 'TMAX', '40', '', ''], ['S', '20200102', 'PRCP', '0', 'P', ''], ['S', '20200102', 'TMAX', '80', '', 'X']]
        raw = pd.DataFrame(rows, columns=['station_id', 'date', 'element', 'value', 'mflag', 'qflag'])
        w, s, b = clean_weather_elements(raw)
        self.assertEqual(len(w), 1)
        self.assertEqual(w.prcp_mm.iloc[0], 1.2)
        self.assertTrue(w.tmin_c.isna().all())
        self.assertEqual(len(b), 1)
        self.assertEqual(s['weather_quality_flagged_elements'], 1)

    def test_duplicate_weather_is_not_silently_resolved(self):
        r = pd.DataFrame([['S', '20200101', 'PRCP', '12', '', '']] * 2, columns=['station_id', 'date', 'element', 'value', 'mflag', 'qflag'])
        with self.assertRaises(ValueError):
            clean_weather_elements(r)

    def test_daily_eligible_station_tie_missing_and_distance(self):
        stations = pd.DataFrame([[1, 'A', 34, -118], [2, 'B', 34, -118], [3, 'C', 34.001, -118]], columns=['station_key', 'station_id', 'latitude', 'longitude'])
        w = pd.DataFrame([[1, 20200101, 1, 10, 20], [2, 20200101, 2, 10, 20], [3, 20200101, np.nan, 10, 20]], columns=['station_key', 'date_key', 'prcp_mm', 'tmin_c', 'tmax_c'])
        c = pd.DataFrame([[20200101, 34, -118, True], [20200102, 34, -118, True], [20200101, 40, -118, True], [20200101, np.nan, np.nan, False]], columns=['date_key', 'latitude', 'longitude', 'geocoded'])
        matched = WeatherMatcher(stations, w).match(c)
        self.assertEqual(matched.station_key.iloc[0], 1)
        self.assertEqual(matched.station_distance_m.iloc[0], 0)
        self.assertTrue(matched.station_key.iloc[1:].isna().all())

# SECTION 4: small rule checks. These do not reload the database.
def check_rules():
    """Run the five cleaning and matching checks before any full load."""
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Rules)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise RuntimeError('Cleaning or matching checks failed. Warehouse retained.')

class RunOutput:
    """Send progress and errors to the screen and the same run log."""
    def __init__(self, screen, log_file):
        self.screen = screen
        self.log_file = log_file
    def write(self, message):
        self.screen.write(message)
        self.log_file.write(message)
        self.log_file.flush()
        return len(message)
    def flush(self):
        self.screen.flush()
        self.log_file.flush()

# SECTION 5: one full run, with one log file.
def run_full_etl():
    """Check, prepare, load, validate, and export the warehouse schema."""
    import getpass
    import subprocess
    import traceback
    global ALLOW_TRAFFIC_UPDATES, TRAFFIC_CHANGED
    ALLOW_TRAFFIC_UPDATES = True
    TRAFFIC_CHANGED = False
    password_requested = 'TRAFFIC_PASSWORD' not in os.environ
    if password_requested:
        os.environ['TRAFFIC_PASSWORD'] = database_password()
    log_path = ROOT / 'Support/ETL_Run.log'
    original_out, original_error = sys.stdout, sys.stderr
    try:
        # Overwrite the previous run log: only one current log is retained.
        with log_path.open('w', encoding='utf-8') as output:
            sys.stdout = RunOutput(original_out, output)
            sys.stderr = RunOutput(original_error, output)
            try:
                log('Full Python ETL started')
                check_rules()
                log('STEP 1: extract, clean, match sources, and prepare tables')
                import download_sources
                with closing(sqlite3.connect(Path(CFG['sqlite_path']).as_uri()+'?mode=ro',uri=True)) as source:
                    latest_year=int(source.execute('SELECT max(substr(collision_date,1,4)) FROM collisions').fetchone()[0])
                snapshot_path=ROOT/'Support/Generated/Checks/current_manifest.json'
                snapshot=json.loads(snapshot_path.read_text(encoding='utf-8')) if snapshot_path.exists() else {}
                previous_end=snapshot.get('source_end_date','2021-06-03')
                with closing(sqlite3.connect(Path(CFG['sqlite_path']).as_uri()+'?mode=ro',uri=True)) as source:
                    source_end=str(source.execute('SELECT max(collision_date) FROM collisions').fetchone()[0])[:10]
                refresh=CFG['weather_files'] if source_end>previous_end else []
                downloads=download_sources.ensure_sources(CFG,ROOT,latest_year,refresh_paths=refresh)
                prepare()
                if downloads or latest_year>2021:
                    prepared_path=ROOT/'Support/Generated/Checks/python_run.json'
                    prepared=json.loads(prepared_path.read_text());prepared['sources_updated']=True
                    prepared_path.write_text(json.dumps(prepared,indent=2))
                log('STEP 2: load replacement schemas, validate, and publish')
                load_and_publish()
                import shutil
                dump_path = shutil.which('pg_dump')
                if not dump_path and os.name == 'nt':
                    candidates = sorted(Path(os.getenv('ProgramFiles','C:/Program Files')).glob('PostgreSQL/*/bin/pg_dump.exe'), reverse=True)
                    dump_path = str(candidates[0]) if candidates else None
                pg_dump = Path(dump_path) if dump_path else Path('__pg_dump_unavailable__')
                if pg_dump.exists():
                    log('STEP 3: export the PostgreSQL schema')
                    environment = os.environ.copy()
                    environment['PGPASSWORD'] = os.environ['TRAFFIC_PASSWORD']
                    subprocess.run([str(pg_dump), '-h', os.getenv('PGHOST') or CFG.get('database',{}).get('host','localhost'),
                        '-p', os.getenv('PGPORT') or str(CFG.get('database',{}).get('port',5432)), '-U', os.getenv('PGUSER') or CFG.get('database',{}).get('user','postgres'),
                        '-d', os.getenv('PGDATABASE') or CFG.get('database',{}).get('name','warehouse'), '--schema-only',
                        '--no-owner', '--no-privileges', '--schema=landing', '--schema=staging',
                        '--schema=dw', '--schema=mart', '--schema=audit',
                        '-f', str(ROOT/'Support/Generated/Checks/Database_Schema.sql')],
                        env=environment, check=True, capture_output=True, text=True)
                import download_sources
                download_sources.record_download_catalog(ROOT)
                log('Full Python ETL completed successfully')
            except Exception:
                log('Run failed. Read the error below and the saved validation status.')
                traceback.print_exc()
                raise
            finally:
                sys.stdout, sys.stderr = original_out, original_error
    finally:
        if password_requested:
            os.environ.pop('TRAFFIC_PASSWORD', None)
    print('Run log:', log_path)

def query_frame(db, statement, parameters=None):
    """Read a PostgreSQL result as a pandas table, including its column names."""
    cursor = db.execute(statement, parameters)
    return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def upsert_frame(db, schema, table, frame, keys):
    """COPY a batch into temporary storage, then insert new keys or update changed values."""
    if frame.empty:
        return 0
    columns = list(frame.columns)
    temporary = 'batch_' + table
    target = sql.SQL('{}.{}').format(sql.Identifier(schema), sql.Identifier(table))
    db.execute(sql.SQL('DROP TABLE IF EXISTS pg_temp.{}').format(sql.Identifier(temporary)))
    db.execute(sql.SQL('CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP').format(sql.Identifier(temporary), target))
    names = sql.SQL(',').join(map(sql.Identifier, columns))
    command = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')").format(sql.Identifier(temporary), names)
    with db.cursor().copy(command) as copy:
        copy.write(frame.to_csv(index=False, na_rep='', lineterminator='\n'))
    changes = [name for name in columns if name not in keys]
    compared = [name for name in changes if name != 'batch_id']
    assignments = sql.SQL(',').join(sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(name), sql.Identifier(name)) for name in changes)
    conflict = sql.SQL(',').join(map(sql.Identifier, keys))
    if changes:
        condition = sql.SQL(' OR ').join(sql.SQL('destination.{} IS DISTINCT FROM EXCLUDED.{}').format(sql.Identifier(name), sql.Identifier(name)) for name in compared)
        action = sql.SQL('DO UPDATE SET {} WHERE {}').format(assignments, condition)
    else:
        action = sql.SQL('DO NOTHING')
    statement = sql.SQL('INSERT INTO {} AS destination ({}) SELECT {} FROM {} WHERE true ON CONFLICT ({}) {}').format(target, names, names, sql.Identifier(temporary), conflict, action)
    return db.execute(statement).rowcount


def batch_weather(db, schemas, stations, dates, collisions=None):
    """Reuse stored weather, and read only requested dates from raw station files when needed."""
    dates = sorted(set(int(day) for day in dates))
    if not dates:
        return pd.DataFrame(columns=WEATHER)
    weather = query_frame(db, f"SELECT * FROM {schemas['dw']}.fact_weather_daily WHERE date_key=ANY(%s)", (dates,))
    missing = set(dates) - set(weather.date_key)
    unmatched=None
    if collisions is not None and len(collisions):
        preliminary=WeatherMatcher(stations,weather).match(collisions)
        unmatched=collisions.loc[collisions.geocoded & preliminary.station_key.isna()]
        missing.update(int(day) for day in unmatched.date_key)
    if not missing:
        return weather
    requested = {str(day) for day in missing}
    nearby_ids=None
    if unmatched is not None and len(unmatched):
        # Only download stations that could match these crash coordinates within 50 km.
        tree=cKDTree(xyz(stations.latitude,stations.longitude))
        limit=2*EARTH_RADIUS*np.sin(CFG.get('max_station_distance_m',50000)/(2*EARTH_RADIUS))
        groups=tree.query_ball_point(xyz(unmatched.latitude,unmatched.longitude),limit+1e-8)
        positions=sorted({position for group in groups for position in group})
        nearby_ids=set(stations.iloc[positions].station_id)
    refresh_paths=[path for path in CFG['weather_files'] if nearby_ids is None or Path(path).name.split('.')[0] in nearby_ids]
    import download_sources
    downloads=download_sources.ensure_sources(CFG,ROOT,max(missing)//10000,refresh_paths=refresh_paths)
    station_keys = dict(zip(stations.station_id, stations.station_key))
    pieces = []
    names = ['station_id', 'date', 'element', 'value', 'mflag', 'qflag', 'sflag', 'obs_time']
    for index, path in enumerate(CFG['weather_files'], 1):
        sid = Path(path).name.split('.')[0]
        if sid not in station_keys or (nearby_ids is not None and sid not in nearby_ids):
            continue
        selected = []
        for raw in pd.read_csv(path, header=None, names=names, dtype=str, keep_default_na=False, compression='gzip', chunksize=200000):
            selected.append(raw[raw.date.isin(requested) & raw.element.isin(['PRCP', 'TMIN', 'TMAX'])])
        raw = pd.concat(selected, ignore_index=True)
        if raw.empty:
            continue
        if not raw.station_id.eq(sid).all():
            raise ValueError('Station ID does not match weather filename')
        wide, _, rejected = clean_weather_elements(raw, min(requested), max(requested))
        wide['station_key'] = station_keys[sid]
        wide['source_key'] = 2
        pieces.append(wide[list(WEATHER)])
        if len(rejected):
            log(f'Batch weather: {sid}, {len(rejected)} conflicting temperature records left missing')
        if index % 100 == 0:
            log(f'Batch weather: checked {index} station files for new observation dates')
    if pieces:
        new = pd.concat(pieces, ignore_index=True)
        upsert_frame(db, schemas['dw'], 'fact_weather_daily', new, ['station_key', 'date_key'])
        # Replace reread station-days instead of returning duplicate keys.
        combined=pd.concat([weather,new],ignore_index=True)
        weather=combined.drop_duplicates(['station_key','date_key'],keep='last')
    weather.attrs['downloaded_sources']=downloads
    return weather


def refresh_county_rates(db, schemas, affected, source_last_date):
    """Recalculate only affected county-years in Python, including previous collisions."""
    for county_key, year in sorted(affected):
        rows = query_frame(db, f"SELECT killed, injured, severe_or_fatal, weather_matched FROM {schemas['mart']}.collision_context WHERE county_key=%s AND year=%s", (county_key, year))
        county = db.execute(f"SELECT fips,county_name FROM {schemas['dw']}.dim_county WHERE county_key=%s", (county_key,)).fetchone()
        population = db.execute(f"SELECT population,vintage FROM {schemas['dw']}.fact_population_annual WHERE county_key=%s AND year_key=%s", (county_key, year)).fetchone()
        previous = db.execute(f"SELECT partial_year FROM {schemas['mart']}.safety_county_year WHERE fips=%s AND year=%s", (county[0], year)).fetchone()
        partial = bool(previous[0]) if previous else True
        # A daily append cannot prove that a whole calendar year is complete.
        count = len(rows)
        number_of_residents = population[0] if population else None
        values = dict(fips=county[0], county_name=county[1], year=year, collisions=count,
            severe_fatal_collisions=int(rows.severe_or_fatal.sum()),
            killed=rows.killed.sum(min_count=1), injured=rows.injured.sum(min_count=1),
            missing_killed_counts=int(rows.killed.isna().sum()), missing_injured_counts=int(rows.injured.isna().sum()),
            population=number_of_residents, vintage=population[1] if population else None,
            weather_match_fraction=float(rows.weather_matched.sum())/count if count else None,
            partial_year=partial, collisions_per_100k=100000*count/number_of_residents if number_of_residents and not partial else None)
        frame = pd.DataFrame([values], columns=RATES)
        for name in ['collisions', 'severe_fatal_collisions', 'killed', 'injured', 'missing_killed_counts', 'missing_injured_counts', 'population']:
            frame[name] = frame[name].astype('Int64')
        upsert_frame(db, schemas['mart'], 'safety_county_year', frame, ['fips', 'year'])


def run_date_batch(from_date, to_date=None, sqlite_path=None, dry_run=False, schemas=None):
    """Read an inclusive collision-date window and publish one atomic, repeatable batch."""
    schemas = schemas or {'dw': 'dw', 'mart': 'mart', 'audit': 'audit'}
    import re
    if not all(re.fullmatch('[a-z_][a-z_0-9]*', name) for name in schemas.values()):
        raise ValueError('Invalid internal schema name')
    start_date = dt.date.fromisoformat(from_date)
    requested_end = dt.date.fromisoformat(to_date) if to_date else None
    if requested_end and requested_end < start_date:
        raise ValueError('--to must be on or after --from')
    source_path = Path(sqlite_path or CFG['sqlite_path']).resolve()
    uri = source_path.as_uri() + '?mode=ro'
    date_column = '"' + CFG['columns']['date'].replace('"', '""') + '"'
    projection = ','.join('"' + name.replace('"', '""') + '"' for name in CFG['traffic_projection'])
    receipt = dict(batch_id=str(uuid.uuid4()), mode='incremental', from_date=from_date,
        source_file=str(source_path), source_bytes=source_path.stat().st_size,
        source_modified_ns=source_path.stat().st_mtime_ns, selected=0, accepted=0, rejected=0,
        new_cases=0, changed_fact_rows=0, changed_mart_rows=0, status='running')
    selected_digest = hashlib.sha256()
    with closing(sqlite3.connect(uri, uri=True)) as source:
        maximum = source.execute(f'SELECT max(date({date_column})) FROM collisions').fetchone()[0]
        if maximum is None:
            receipt['status'] = 'no_records'
            return receipt
        last_date = dt.date.fromisoformat(str(maximum)[:10])
        end_date = min(requested_end, last_date) if requested_end else last_date
        receipt['source_latest_date'] = last_date.isoformat()
        receipt['to_date'] = end_date.isoformat()
        upper = (end_date + dt.timedelta(days=1)).isoformat()
        statement = f'SELECT {projection} FROM collisions WHERE {date_column} >= ? AND {date_column} < ?'
        bounds = (start_date.isoformat(), upper)
        if dry_run:
            receipt['selected'] = source.execute(f'SELECT count(*) FROM collisions WHERE {date_column} >= ? AND {date_column} < ?', bounds).fetchone()[0]
            receipt['status'] = 'preview_only'
            log(json.dumps(receipt, indent=2))
            return receipt
        selected_count=source.execute(f'SELECT count(*) FROM collisions WHERE {date_column} >= ? AND {date_column} < ?',bounds).fetchone()[0]
        if not selected_count:
            receipt['status']='no_records'
            log('No matching records; no downloads or database changes.')
            return receipt
        import download_sources
        receipt['downloaded_sources']=download_sources.ensure_sources(CFG,ROOT,end_date.year)
        with connect() as db:
            with db.transaction():
                # Serialize writers so dimension keys and repeated batches stay consistent.
                db.execute('SELECT pg_advisory_xact_lock(5442026)')
                counties = query_frame(db, f"SELECT * FROM {schemas['dw']}.dim_county")
                stations = query_frame(db, f"SELECT * FROM {schemas['dw']}.dim_station")
                contexts_frame = query_frame(db, f"SELECT * FROM {schemas['dw']}.dim_context")
                contexts = {(r.lighting,r.road_surface,r.reported_weather):r.context_key for r in contexts_frame.itertuples()}
                next_context = max(contexts.values(), default=0) + 1
                # Prepare Census rows in Python without replacing full staging files.
                population_counties, all_population=clean_population(write_outputs=False)
                lookup=dict(zip(population_counties.county_key,population_counties.fips))
                current_keys=dict(zip(counties.fips,counties.county_key))
                required=all_population[all_population.year_key.between(start_date.year,end_date.year)].copy()
                required['county_key']=required.county_key.map(lookup).map(current_keys)
                upsert_frame(db,schemas['dw'],'dim_year',pd.DataFrame({'year_key':required.year_key.unique()}),['year_key'])
                upsert_frame(db,schemas['dw'],'fact_population_annual',required[list(POP)],['county_key','year_key'])
                population = query_frame(db, f"SELECT county_key,year_key AS year,population,vintage FROM {schemas['dw']}.fact_population_annual")
                fips_keys = dict(zip(counties.fips, counties.county_key))
                names = dict(zip(counties.county_key, counties.county_name))
                fips = dict(zip(counties.county_key, counties.fips))
                cross = pd.read_csv(CFG['county_crosswalk_path'], dtype=str, keep_default_na=False)
                county_keys = {r.source_county:fips_keys[r.fips] for r in cross.itertuples()}
                affected = set()
                seen_ids = set()
                for raw in pd.read_sql_query(statement, source, params=bounds, chunksize=100000):
                    receipt['selected'] += len(raw)
                    selected_digest.update(raw.to_csv(index=False).encode('utf-8'))
                    c, rejects = clean_traffic(raw, START, end_date.isoformat())
                    receipt['rejected'] += len(rejects)
                    if len(rejects):
                        log('Batch rejects: ' + json.dumps(rejects[['case_id','reject_reason']].to_dict('records')))
                    if c.empty:
                        continue
                    if c.case_id.duplicated().any() or set(c.case_id) & seen_ids:
                        raise ValueError('Duplicate case IDs in the selected SQLite batch')
                    seen_ids.update(c.case_id)
                    previous = db.execute(f"SELECT case_id,county_key,year FROM {schemas['mart']}.collision_context WHERE case_id=ANY(%s)", (c.case_id.tolist(),)).fetchall()
                    receipt['new_cases'] += len(c) - len(previous)
                    affected.update((row[1],row[2]) for row in previous)
                    c['county_key'] = category(c['county']).map(county_keys).fillna(0).astype('int32')
                    new_contexts = []
                    for combo in dict.fromkeys(zip(c.lighting,c.road_surface,c.reported_weather)):
                        if combo not in contexts:
                            contexts[combo] = next_context
                            new_contexts.append((next_context,*combo))
                            next_context += 1
                    upsert_frame(db, schemas['dw'], 'dim_context', pd.DataFrame(new_contexts,columns=DIMS['dim_context']), ['context_key'])
                    c['context_key'] = [contexts[combo] for combo in zip(c.lighting,c.road_surface,c.reported_weather)]
                    c['source_key'] = 1
                    c['collision_count'] = 1
                    c['batch_id'] = receipt['batch_id']
                    for name in ['primary_road','secondary_road','primary_collision_factor','type_of_collision']:
                        c[name] = category(c[name])
                    calendar = pd.DataFrame({'full_date':c.full_date.drop_duplicates().sort_values()})
                    calendar['date_key'] = calendar.full_date.dt.strftime('%Y%m%d').astype(int)
                    for name in ['year','month','quarter']:
                        calendar[name] = getattr(calendar.full_date.dt,name)
                    calendar['iso_weekday'] = calendar.full_date.dt.dayofweek + 1
                    calendar['is_weekend'] = calendar.iso_weekday > 5
                    calendar['full_date'] = calendar.full_date.dt.strftime('%Y-%m-%d')
                    upsert_frame(db, schemas['dw'], 'dim_date', calendar[list(DIMS['dim_date'])], ['date_key'])
                    upsert_frame(db, schemas['dw'], 'dim_year', pd.DataFrame({'year_key':c.year.unique()}), ['year_key'])
                    weather = batch_weather(db, schemas, stations, c.loc[c.geocoded,'date_key'],c)
                    receipt['downloaded_sources'].extend(weather.attrs.get('downloaded_sources',[]))
                    c = c.reset_index(drop=True)
                    c = pd.concat([c,WeatherMatcher(stations,weather).match(c)],axis=1).merge(population,on=['county_key','year'],how='left',validate='many_to_one',sort=False)
                    c['population'] = c.population.astype('Int64')
                    c['county_name'] = c.county_key.map(names)
                    c['fips'] = c.county_key.map(fips)
                    c['weather_matched'] = c.station_key.notna()
                    c['population_matched'] = c.population.notna()
                    c['hour'] = c.time_key.where(c.time_key.ne(-1)).astype('Int64')
                    c['severe_or_fatal'] = c.severity.map({'fatal':1,'severe injury':1,'other injury':0,'pain':0,'property damage only':0}).astype('Int64')
                    c['full_date'] = c.full_date.dt.strftime('%Y-%m-%d')
                    receipt['accepted'] += len(c)
                    receipt['changed_fact_rows'] += upsert_frame(db, schemas['dw'], 'fact_collision', c[list(COLLISION)], ['case_id'])
                    receipt['changed_mart_rows'] += upsert_frame(db, schemas['mart'], 'collision_context', c[list(CONTEXT)], ['case_id'])
                    affected.update(zip(c.county_key.astype(int),c.year.astype(int)))
                    log(f"Batch selected {receipt['selected']:,}; accepted {receipt['accepted']:,}; new cases {receipt['new_cases']:,}")
                refresh_county_rates(db, schemas, affected, last_date)
                refresh_snowflake(db, schemas)
                receipt['selected_rows_sha256'] = selected_digest.hexdigest()
                receipt['status'] = 'success'
                matched_keys = 0
                ids = list(seen_ids)
                for offset in range(0,len(ids),100000):
                    matched_keys += db.execute(f"SELECT count(*) FROM {schemas['dw']}.fact_collision f JOIN {schemas['mart']}.collision_context m USING(case_id) WHERE f.case_id=ANY(%s)", (ids[offset:offset+100000],)).fetchone()[0]
                receipt['checks'] = {'selected_equals_accepted_plus_rejected':receipt['selected']==receipt['accepted']+receipt['rejected'], 'all_accepted_keys_in_fact_and_mart':matched_keys==receipt['accepted']}
                if not all(receipt['checks'].values()):
                    raise ValueError('Batch row reconciliation failed; transaction rolled back')
                db.execute(f"INSERT INTO {schemas['audit']}.batches(batch_id,started_at,status,details) VALUES(%s,now(),%s,%s)", (receipt['batch_id'],'success',json.dumps(receipt)))
    log('Batch completed: ' + json.dumps(receipt))
    return receipt


def run_batch_command(from_date=None, to_date=None, sqlite_path=None, dry_run=False, daily=False):
    """Run a date batch using the same single log; keep its latest receipt in Checks."""
    import getpass,traceback
    asked = 'TRAFFIC_PASSWORD' not in os.environ and (not dry_run or daily)
    if asked:
        os.environ['TRAFFIC_PASSWORD'] = database_password()
    original_out,original_error = sys.stdout,sys.stderr
    try:
        with (ROOT/'Support/ETL_Run.log').open('w',encoding='utf-8') as output:
            sys.stdout,sys.stderr = RunOutput(original_out,output),RunOutput(original_error,output)
            try:
                if daily:
                    with connect() as db:
                        latest = db.execute('SELECT max(date_key) FROM dw.fact_collision').fetchone()[0]
                    if latest is None:
                        raise ValueError('Run --all first to initialize the warehouse')
                    lookback = int(CFG.get('batch_settings',{}).get('daily_lookback_days',2))
                    from_date = (dt.datetime.strptime(str(latest),'%Y%m%d').date()-dt.timedelta(days=max(0,lookback-1))).isoformat()
                log('Incremental batch started; from date is inclusive')
                receipt = run_date_batch(from_date,to_date,sqlite_path,dry_run)
                if not dry_run:
                    (ROOT/'Support/Generated/Checks/batch_run.json').write_text(json.dumps(receipt,indent=2))
                    if receipt['changed_fact_rows'] or receipt['changed_mart_rows']:
                        path=ROOT/'Support/Generated/Checks/current_manifest.json'
                        manifest=json.loads(path.read_text())
                        manifest['status']='incremental_updated'
                        manifest['last_incremental_batch_id']=receipt['batch_id']
                        manifest['full_validation_required']=True
                        path.write_text(json.dumps(manifest,indent=2))
                        log('Full-validation snapshot is stale. Run --all to refresh the complete warehouse checks.')
            except Exception:
                traceback.print_exc()
                raise
            finally:
                sys.stdout,sys.stderr=original_out,original_error
    finally:
        if asked:
            os.environ.pop('TRAFFIC_PASSWORD',None)


def test_date_batches():
    """Prove insert, repeat, correction, missing context, and rollback using isolated data."""
    import tempfile, getpass
    from unittest.mock import patch
    import download_sources
    asked = 'TRAFFIC_PASSWORD' not in os.environ
    if asked:
        os.environ['TRAFFIC_PASSWORD'] = database_password()
    prefix = 'testbatch_' + uuid.uuid4().hex[:8]
    schemas = {key: prefix + '_' + key for key in ['dw','mart','audit']}
    try:
        with patch.object(download_sources,'ensure_sources',return_value=[]), connect() as db:
            with db.transaction():
                for name in schemas.values():
                    db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(name)))
                for table in list(DIMS) + ['fact_collision','fact_population_annual','fact_weather_daily']:
                    db.execute(f"CREATE TABLE {schemas['dw']}.{table} (LIKE dw.{table} INCLUDING ALL)")
                    if table in DIMS or table == 'fact_population_annual':
                        db.execute(f"INSERT INTO {schemas['dw']}.{table} SELECT * FROM dw.{table}")
                for dimension in ['date','time','county','context','source']:
                    db.execute(f"ALTER TABLE {schemas['dw']}.fact_collision ADD FOREIGN KEY({dimension}_key) REFERENCES {schemas['dw']}.dim_{dimension}({dimension}_key)")
                for table in ['collision_context','safety_county_year']:
                    db.execute(f"CREATE TABLE {schemas['mart']}.{table} (LIKE mart.{table} INCLUDING ALL)")
                db.execute(f"CREATE TABLE {schemas['audit']}.batches (LIKE audit.batches INCLUDING ALL)")
                latest = db.execute('SELECT max(date_key) FROM dw.fact_collision').fetchone()[0]
            next_day = (dt.datetime.strptime(str(latest),'%Y%m%d').date()+dt.timedelta(days=1)).isoformat()
            with tempfile.TemporaryDirectory(prefix='batch_demo_',dir=ROOT/'Support/Generated') as directory:
                example = Path(directory)/'two_dummy_cases.sqlite'
                uri = Path(CFG['sqlite_path']).resolve().as_uri()+'?mode=ro'
                projection = ','.join('"'+name+'"' for name in CFG['traffic_projection'])
                with closing(sqlite3.connect(uri,uri=True)) as source:
                    cursor=source.execute('SELECT '+projection+' FROM collisions LIMIT 1')
                    template=dict(zip([column[0] for column in cursor.description],cursor.fetchone()))
                with closing(sqlite3.connect(example)) as fixture, fixture:
                    fixture.execute('CREATE TABLE collisions ('+','.join('"'+name+'"' for name in template)+')')
                    fields=list(template)
                    insert='INSERT INTO collisions VALUES('+','.join('?' for _ in fields)+')'
                    rows=[]
                    for index in range(2):
                        row=template.copy()
                        row.update(case_id='DUMMY_BATCH_'+str(index+1),collision_date=next_day,
                            collision_time='0000',latitude=None,longitude=None,
                            collision_severity='fatal' if index==0 else 'other injury',
                            killed_victims=1 if index==0 else 0,injured_victims=0 if index==0 else 2)
                        rows.append(row)
                        fixture.execute(insert,[row[name] for name in fields])
                import gzip
                station=db.execute(f"SELECT station_id,latitude,longitude FROM {schemas['dw']}.dim_station ORDER BY station_key LIMIT 1").fetchone()
                weather_file=Path(directory)/(station[0]+'.csv.gz')
                with gzip.open(weather_file,'wt',encoding='utf-8',newline='') as weather_output:
                    writer=csv.writer(weather_output)
                    for element,value in [('PRCP','12'),('TMIN','100'),('TMAX','200')]:
                        writer.writerow([station[0],next_day.replace('-',''),element,value,'','','S',''])
                with closing(sqlite3.connect(example)) as fixture,fixture:
                    fixture.execute("UPDATE collisions SET latitude=?,longitude=? WHERE case_id='DUMMY_BATCH_1'",station[1:])
                weather_original=CFG['weather_files']
                CFG['weather_files']=[str(weather_file)]
                first=run_date_batch(next_day,sqlite_path=example,schemas=schemas)
                measured=db.execute(f"SELECT weather_matched,prcp_mm,tmin_c,tmax_c FROM {schemas['mart']}.collision_context WHERE case_id='DUMMY_BATCH_1'").fetchone()
                assert measured==(True,1.2,10.0,20.0)
                assert first['new_cases']==2 and first['changed_fact_rows']==2
                repeated=run_date_batch(next_day,sqlite_path=example,schemas=schemas)
                assert repeated['new_cases']==0 and repeated['changed_fact_rows']==0 and repeated['changed_mart_rows']==0
                assert db.execute(f"SELECT count(*) FROM {schemas['dw']}.fact_collision").fetchone()[0]==2
                assert db.execute(f"SELECT sum(collisions),sum(injured) FROM {schemas['mart']}.safety_county_year").fetchone()==(2,2)
                # Correct a case and move it to a year not covered by Census files.
                future_date = dt.date(max(2023, int(next_day[:4])+2),1,1).isoformat()
                with closing(sqlite3.connect(example)) as fixture, fixture:
                    fixture.execute("UPDATE collisions SET injured_victims=5,collision_date=? WHERE case_id='DUMMY_BATCH_2'", (future_date,))
                corrected=run_date_batch(next_day,sqlite_path=example,schemas=schemas)
                assert corrected['new_cases']==0 and corrected['changed_fact_rows']==1
                stored=db.execute(f"SELECT injured,population,weather_matched FROM {schemas['mart']}.collision_context WHERE case_id='DUMMY_BATCH_2'").fetchone()
                estimate=db.execute(f"SELECT population FROM {schemas['dw']}.fact_population_annual WHERE county_key=(SELECT county_key FROM {schemas['mart']}.collision_context WHERE case_id='DUMMY_BATCH_2') AND year_key=%s",(int(future_date[:4]),)).fetchone()
                assert stored==(5,estimate[0] if estimate else None,False)
                assert db.execute(f"SELECT sum(collisions),sum(injured) FROM {schemas['mart']}.safety_county_year").fetchone()==(2,5)
                assert db.execute(f"SELECT collisions_per_100k FROM {schemas['mart']}.safety_county_year WHERE year=%s", (int(future_date[:4]),)).fetchone()[0] is None
                preview=run_date_batch(next_day,to_date=next_day,sqlite_path=example,dry_run=True,schemas=schemas)
                assert preview['selected']==1 and preview['status']=='preview_only'
                # An error after upserts must roll back the whole new batch.
                third=rows[0].copy();third['case_id']='DUMMY_BATCH_3'
                with closing(sqlite3.connect(example)) as fixture, fixture:
                    fixture.execute(insert,[third[name] for name in fields])
                with patch.object(sys.modules[__name__],'refresh_county_rates',side_effect=RuntimeError('Intentional rollback test')):
                    try:
                        run_date_batch(next_day,sqlite_path=example,schemas=schemas)
                    except RuntimeError as error:
                        assert str(error)=='Intentional rollback test'
                    else:
                        raise AssertionError('Rollback test did not fail as expected')
                assert db.execute(f"SELECT count(*) FROM {schemas['dw']}.fact_collision").fetchone()[0]==2
                assert db.execute(f"SELECT count(*) FROM {schemas['mart']}.collision_context").fetchone()[0]==2
                log('PASS: two new cases, repeat without duplicates, correction, new-year population handling, Python aggregates, and atomic rollback')
    finally:
        if 'weather_original' in locals():
            CFG['weather_files']=weather_original
        with connect() as db:
            for name in reversed(list(schemas.values())):
                db.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(name)))
        if asked:
            os.environ.pop('TRAFFIC_PASSWORD',None)


def make_demo_source():
    """Create one clearly marked collision in a separate tiny SQLite file.
    The full SWITRS source is opened read-only and never changed.
    The date is one day after its latest collision. Coordinates stay missing
    so this simple demonstration does not imply invented weather or location.
    """
    with closing(sqlite3.connect(Path(CFG['sqlite_path']).resolve().as_uri()+'?mode=ro',uri=True)) as source:
        latest = str(source.execute('SELECT max(collision_date) FROM collisions').fetchone()[0])[:10]
        date = (dt.date.fromisoformat(latest)+dt.timedelta(days=1)).isoformat()
        create = source.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='collisions'").fetchone()[0]
        cursor=source.execute('SELECT * FROM collisions LIMIT 1')
        names=[column[0] for column in cursor.description]
        row=dict(zip(names,cursor.fetchone()))
    row.update(case_id='DEMO_PORTABLE_1',collision_date=date,collision_severity='property damage only',
               killed_victims=0,injured_victims=0,latitude=None,longitude=None)
    path=ROOT/'Original_Data/demo.sqlite'
    if path.exists():
        with closing(sqlite3.connect(path)) as demo:
            existing=demo.execute('SELECT case_id,collision_date FROM collisions').fetchall()
        if existing != [('DEMO_PORTABLE_1',date)]:
            raise ValueError('Existing demo.sqlite has different records; move it aside first')
    else:
        with closing(sqlite3.connect(path)) as demo, demo:
            demo.execute(create)
            fields=','.join('"'+name.replace('"','""')+'"' for name in names)
            demo.execute('INSERT INTO collisions ('+fields+') VALUES ('+','.join('?' for name in names)+')',[row[name] for name in names])
    print('Demo SQLite:',path,'; one case DEMO_PORTABLE_1; date:',date)
    return path,date


def run_demo_one():
    """Run exactly one demo record through the normal date-batch process."""
    path,date=make_demo_source()
    run_batch_command(date,date,str(path))
    with connect() as db:
        row=db.execute("SELECT case_id,full_date,population,weather_matched FROM mart.collision_snowflake WHERE case_id='DEMO_PORTABLE_1'").fetchone()
    if row is None:
        raise RuntimeError('Demo case did not reach the combined view')
    print('Loaded demo:',row)
    print('Repeat --demo-one to check that no duplicate is added.')
    print('Run --all using the original SQLite to remove the demo from this database.')


def main():
    """Choose all records, a date batch, daily continuation, or teaching checks."""
    parser = argparse.ArgumentParser(description='California traffic Python ETL')
    commands = parser.add_mutually_exclusive_group(required=True)
    commands.add_argument('--all', '--run', dest='all_records', action='store_true', help='Full three-source rebuild; allow new traffic records')
    commands.add_argument('--from', dest='from_date', metavar='YYYY-MM-DD', help='Incremental upsert from this date, inclusive')
    commands.add_argument('--daily', action='store_true', help='Reload the last configured days plus newer dates')
    commands.add_argument('--demo-one',action='store_true',help='Create one separate SQLite demo record and load it as a date batch')
    commands.add_argument('--test', action='store_true', help='Run small cleaning checks')
    commands.add_argument('--test-batch', action='store_true', help='Test two dummy cases in isolated SQLite and PostgreSQL schemas')
    commands.add_argument('--prepare', action='store_true', help='Prepare staging only')
    commands.add_argument('--load', action='store_true', help='Load existing full staging')
    parser.add_argument('--to', dest='to_date', metavar='YYYY-MM-DD', help='Optional inclusive ending date for a batch')
    parser.add_argument('--sqlite', dest='sqlite_path', help='Alternative SQLite source for a date batch')
    parser.add_argument('--dry-run', action='store_true', help='Preview how many source rows a batch selects; no database changes')
    parser.add_argument('--limit', type=int, help='Teaching sample for preparation only')
    args = parser.parse_args()
    is_batch = args.from_date is not None or args.daily
    if args.limit is not None and (not args.prepare or args.limit <= 0):
        parser.error('--limit must be positive and used only with --prepare')
    if (args.to_date or args.sqlite_path or args.dry_run) and not is_batch:
        parser.error('--to, --sqlite and --dry-run are for date batches only')
    for value in [args.from_date,args.to_date]:
        if value:
            try:
                if dt.date.fromisoformat(value).isoformat()!=value:
                    raise ValueError()
            except ValueError:
                parser.error('Dates must use YYYY-MM-DD, for example 2021-06-04')
    if args.all_records:
        run_full_etl()
    elif is_batch:
        run_batch_command(args.from_date,args.to_date,args.sqlite_path,args.dry_run,args.daily)
    elif args.demo_one:
        run_demo_one()
    elif args.test:
        check_rules()
    elif args.test_batch:
        test_date_batches()
    elif args.prepare:
        prepare(args.limit)
    elif args.load:
        load_and_publish()

if __name__ == '__main__':
    main()
