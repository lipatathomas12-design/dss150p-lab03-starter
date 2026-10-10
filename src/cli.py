import argparse
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
<<<<<<< HEAD
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import upsert_curated, load_partition
from src.validate.quality import run_quality_checks
from src.benchmark.storage import run_benchmarks
=======
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
<<<<<<< HEAD
    
=======
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
<<<<<<< HEAD
    
    b = sub.add_parser('benchmark')
    b.add_argument('--repeats', type=int, default=5)
    
    p = sub.add_parser('load-partition')
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--month', type=int, required=True)
    
    sub.add_parser('run-all')
    
    args = parser.parse_args()
    run_id = new_run_id()
=======
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

<<<<<<< HEAD
    elif args.command == 'extract':
        print(f'Starting extraction with run_id: {run_id}')
        raw_data = extract_sources(run_id)
        print(f'Successfully extracted sources. Raw snapshots saved.')

    elif args.command == 'transform':
        print(f'Starting transformation with run_id: {run_id}')
        raw_data = extract_sources(run_id)
        stg_df, quarantine_df = build_staging(raw_data, run_id)
        curated_df, orphans_df = build_curated(stg_df, run_id)
        print(f'Transformation complete. Curated rows: {len(curated_df)}, Quarantine rows: {len(quarantine_df)}, Orphans: {len(orphans_df)}')

    elif args.command == 'load':
        print(f'Starting load phase with run_id: {run_id}')
        raw_data = extract_sources(run_id)
        stg_df, _ = build_staging(raw_data, run_id)
        curated_df, _ = build_curated(stg_df, run_id)
        rows_loaded = upsert_curated(curated_df, run_id)
        print(f'Successfully upserted {rows_loaded} rows into PostgreSQL.')

    elif args.command == 'validate':
        print('Running quality validation checks...')
        run_quality_checks()
        print('Quality checks completed.')

    elif args.command == 'benchmark':
        print(f'Running storage benchmarks with {args.repeats} repeats...')
        run_benchmarks(repeats=args.repeats)
        print('Benchmarking completed.')

    elif args.command == 'load-partition':
        print(f'Loading partition for Year: {args.year}, Month: {args.month} with run_id: {run_id}')
        raw_data = extract_sources(run_id)
        stg_df, _ = build_staging(raw_data, run_id)
        curated_df, _ = build_curated(stg_df, run_id)
        rows_loaded = load_partition(curated_df, args.year, args.month, run_id)
        print(f'Successfully loaded {rows_loaded} rows for {args.year}-{args.month:02d}.')

    elif args.command == 'run-all':
        print(f'Executing full pipeline run with run_id: {run_id}')
        raw_data = extract_sources(run_id)
        stg_df, quarantine_df = build_staging(raw_data, run_id)
        curated_df, orphans_df = build_curated(stg_df, run_id)
        rows_loaded = upsert_curated(curated_df, run_id)
        print(f'Pipeline execution complete! Successfully loaded {rows_loaded} rows.')

    else:
        raise NotImplementedError(f'Unknown command: {args.command}')

=======
    # TODO: Wire the modular functions together. Keep orchestration logic thin.
    raise NotImplementedError(f'Wire command: {args.command}')
>>>>>>> 3f0efc07ae7acc17ace3e298eb2ebf5ce91eb6e3

if __name__ == '__main__':
    main()
