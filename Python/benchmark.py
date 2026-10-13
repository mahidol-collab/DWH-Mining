"""Benchmark existing warehouse designs and export reproducible evidence.
Does not change warehouse tables/indexes; use SQL/Benchmark_Preparation.sql separately.
No import of pipeline.py: importing it would execute project setup side effects.
"""
from pathlib import Path
import argparse, csv, datetime as dt, getpass, json, os, platform, statistics
ROOT=Path(__file__).resolve().parents[1]

def query_text(query, table, phase, qid):
    q=query.replace('dw.fact_collision',table)
    if phase=='partitioning':
        filters={'Q2':'f.date_key BETWEEN 20150101 AND 20201231',
                 'Q3':'f.date_key BETWEEN 20010101 AND 20201231',
                 'Q4':'f.date_key BETWEEN 20050101 AND 20201231',
                 'Q5':'f.date_key <= 20201231'}
        if qid in filters:
            q=q.replace('GROUP BY',' AND '+filters[qid]+' GROUP BY',1)
    return q

def executed_partitions(plan):
    names=set()
    def walk(node):
        name=node.get('Relation Name','')
        if name.startswith('fc_p') and node.get('Actual Loops',0)>0:
            names.add(name)
        for child in node.get('Plans',[]): walk(child)
    walk(plan[0]['Plan'])
    return sorted(names)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase',required=True,choices=['baseline','indexed','partitioning'])
    parser.add_argument('--query',default='ALL',choices=['ALL','Q1','Q2','Q3','Q4','Q5'])
    args=parser.parse_args()
    import psycopg
    cfgpath=ROOT/'Support/Settings/config.json'
    if not cfgpath.exists():parser.error('Run download_sources.py --setup first; config.json is missing.')
    cfg=json.loads(cfgpath.read_text(encoding='utf-8'))['database']
    connection=dict(host=os.getenv('PGHOST') or cfg.get('host','localhost'),
        port=int(os.getenv('PGPORT') or cfg.get('port',5432)),
        dbname=os.getenv('PGDATABASE') or cfg.get('name','warehouse'),
        user=os.getenv('PGUSER') or cfg.get('user','postgres'))
    password=os.getenv('TRAFFIC_PASSWORD') or cfg.get('password') or getpass.getpass('PostgreSQL password: ')
    queries=json.loads((ROOT/'Python/benchmark_queries.json').read_text())
    keys=sorted(queries) if args.query=='ALL' else [args.query]
    variants=['B','C'] if args.phase=='partitioning' else [args.phase]
    out=ROOT/'Support/Generated/Benchmarks'/(dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'_'+args.phase+'_'+args.query)
    out.mkdir(parents=True,exist_ok=False)
    manifest={'status':'started','phase':args.phase,'queries':keys,'output':str(out),
       'started_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
       'connection':connection,'client_platform':platform.platform(),'python':platform.python_version(),
       'method':'8 runs per design; runs 1-4 warm-up; median of 5-8; alternating B/C order',
       'scope':'EXPLAIN ANALYZE execution time; excludes planning and client result transfer',
       'hardware_note':'Record server CPU/RAM/storage manually; client may differ from server.',
       'sql':{},'settings':{},'query_validation':{},'notes':['Original and partitioned PK/index configurations may differ.'],}
    timings=[]; summaries=[];comparison=[]
    try:
        with psycopg.connect(**connection,password=password,autocommit=True) as db:
            manifest['server_version']=db.execute('SELECT version()').fetchone()[0]
            manifest['settings']=dict(db.execute("SELECT name,setting FROM pg_settings WHERE name IN ('shared_buffers','work_mem','effective_cache_size','max_parallel_workers_per_gather','jit','random_page_cost','enable_partition_pruning')").fetchall())
            indexes=db.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='dw' ORDER BY tablename,indexname").fetchall()
            with (out/'indexes.csv').open('w',newline='',encoding='utf-8') as f:
                w=csv.writer(f);w.writerow(['table','index','definition']);w.writerows(indexes)
            factidx={r[1] for r in indexes if r[0]=='fact_collision'}
            required={'idx_bench_fc_date_cover','idx_bench_fc_date_time_ctx','idx_bench_fc_county_date_killed'}
            if args.phase=='baseline' and any(r[1].startswith('idx_bench') for r in indexes):
                raise RuntimeError('Benchmark indexes exist. This is not the original baseline. Keep historical baseline evidence or use a separate baseline database.')
            if args.phase!='baseline' and (not required.issubset(factidx) or not any(r[1]=='idx_bench_dd_year' for r in indexes)):
                raise RuntimeError('Required benchmark indexes missing. Review SQL/Benchmark_Preparation.sql.')
            if args.phase=='partitioning' and db.execute("SELECT to_regclass('dw.fact_collision_part')").fetchone()[0] is None:
                raise RuntimeError('Partitioned copy missing. Prepare it first.')
            with (out/'raw_timings.csv').open('w',newline='',encoding='utf-8') as f, (out/'execution_plans.csv').open('w',newline='',encoding='utf-8') as pf:
                w=csv.DictWriter(f,fieldnames=['query_id','design','run_no','is_warmup','execution_ms','planning_ms']);w.writeheader()
                pw=csv.writer(pf);pw.writerow(['query_id','design','run_no','execution_plan'])
                # Keep one consistent read-only database snapshot for validation/timings.
                with db.transaction():
                    db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                    for qid in keys:
                        sqls={v:query_text(queries[qid], 'dw.fact_collision_part' if v=='C' else 'dw.fact_collision',args.phase,qid) for v in variants}
                        manifest['sql'][qid]=sqls
                        if args.phase=='partitioning':
                            b=sqls['B'];c=sqls['C']
                            diff=db.execute('SELECT count(*) FROM ((('+b+') EXCEPT ALL ('+c+')) UNION ALL (('+c+') EXCEPT ALL ('+b+'))) differences').fetchone()[0]
                            manifest['query_validation'][qid]={'differing_output_rows':diff,'scope':'returned query results, including LIMIT where present'}
                            if diff:raise RuntimeError(f'{qid}: B/C result mismatch ({diff} rows); copy may be stale.')
                        for i in range(1,9):
                            order=variants if i%2 else list(reversed(variants))
                            for v in order:
                                plan=db.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+sqls[v]).fetchone()[0]
                                r={'query_id':qid,'design':v,'run_no':i,'is_warmup':i<=4,
                                   'execution_ms':plan[0]['Execution Time'],'planning_ms':plan[0]['Planning Time']}
                                timings.append(r);w.writerow(r);f.flush()
                                if i>=5:pw.writerow([qid,v,i,json.dumps(plan)]);pf.flush()
                                print(f'{qid} {v} run {i}: {r["execution_ms"]:.3f} ms',flush=True)
                        for v in variants:
                            values=[r['execution_ms'] for r in timings if r['query_id']==qid and r['design']==v and not r['is_warmup']]
                            summaries.append(dict(query_id=qid,design=v,measured_runs=len(values),median_ms=statistics.median(values),min_ms=min(values),max_ms=max(values),stddev_ms=statistics.stdev(values)))
                        if args.phase=='partitioning':
                            med={r['design']:r['median_ms'] for r in summaries if r['query_id']==qid}
                            comparison.append(dict(query_id=qid,original_ms=med['B'],partitioned_ms=med['C'],change_percent=100*(med['C']-med['B'])/med['B']))
            # Actual plan partition counts from retained measured plans.
            with (out/'execution_plans.csv').open(newline='',encoding='utf-8') as f:
                scans=[dict(query_id=r['query_id'],design=r['design'],run_no=r['run_no'],partitions_executed=len(executed_partitions(json.loads(r['execution_plan'])))) for r in csv.DictReader(f)]
            save_csv(out/'partitions_executed.csv',scans)
            save_csv(out/'summary.csv',summaries)
            if comparison:save_csv(out/'comparison.csv',comparison)
            storage=[dict(object='original',total_bytes=db.execute("SELECT pg_total_relation_size('dw.fact_collision')").fetchone()[0])]
            if db.execute("SELECT to_regclass('dw.fact_collision_part')").fetchone()[0]:
                total=db.execute("SELECT sum(pg_total_relation_size(relid))::bigint FROM pg_partition_tree('dw.fact_collision_part') WHERE isleaf").fetchone()[0]
                storage.append(dict(object='partitioned_all_leaves',total_bytes=total))
            save_csv(out/'storage.csv',storage)
        manifest['status']='completed'
        print('Evidence saved:',out)
    except Exception as exc:
        manifest['status']='failed';manifest['error']=str(exc)
        raise
    finally:
        manifest['finished_utc']=dt.datetime.now(dt.timezone.utc).isoformat()
        (out/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')

def save_csv(path,rows):
    if not rows:return
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)

if __name__=='__main__':main()
