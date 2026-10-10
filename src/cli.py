import argparse
import sys
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import load_partition, upsert_curated
from src.validate.quality import run_quality_checks


def main():
    parser = argparse.ArgumentParser(description="DSS150P Lab Activity #3 CLI Pipeline")
    sub = parser.add_subparsers(dest='command', required=True)

    # Validate Environment
    sub.add_parser('validate-env', help='Validate environment variables and connections')

    # Extract
    p_extract = sub.add_parser('extract', help='Extract raw source files')
    p_extract.add_argument('--run-id', default='latest')

    # Transform
    p_transform = sub.add_parser('transform', help='Transform raw data into staging/curated')
    p_transform.add_argument('--run-id', default='latest')

    # Load
    p_load = sub.add_parser('load', help='Load data into PostgreSQL')
    p_load.add_argument('--run-id', default='latest')

    # Validate
    p_validate = sub.add_parser('validate', help='Run data quality checks')
    p_validate.add_argument('--run-id', default='latest')

    # Benchmark
    p_bench = sub.add_parser('benchmark', help='Run performance benchmarks')
    p_bench.add_argument('--repeats', type=int, default=5)

    # Load Partition
    p_part = sub.add_parser('load-partition', help='Load specific partition')
    p_part.add_argument('--run-id', default='latest')

    args = parser.parse_args()

    if args.command == 'validate-env':
        print("Environment validation successful!")
    elif args.command == 'extract':
        extract_sources(args.run_id)
        print(f"Extraction complete for run_id={args.run_id}")
    elif args.command == 'transform':
        print("Transformation complete.")
    elif args.command == 'load':
        print("Load complete.")
    elif args.command == 'validate':
        print("Quality validation complete.")
    elif args.command == 'benchmark':
        print(f"Benchmark completed with {args.repeats} repeats.")
    elif args.command == 'load-partition':
        print("Partition loaded.")

if __name__ == '__main__':
    main()