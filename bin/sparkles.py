import argparse
import yaml
from sparkles.runner import SparkRunner


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subparser_name')
    parser_list = subparsers.add_parser('list', help='list datasets')
    parser_run = subparsers.add_parser('run', help='run analysis')
    parser.add_argument('--config-file')
    args = parser.parse_args()
    print(args.subparser_name)
    with open(args.config_file, 'r') as config_file:
        config = yaml.load(config_file)

    sr = SparkRunner(config)
    if args.subparser_name == 'list':
        sr.list_datasets()
    elif args.subparser_name == 'run':
        sr.run_analysis()

if __name__ == '__main__':
    main()
