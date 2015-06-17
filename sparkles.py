import argparse
import yaml
from sparkles.runner import SparkRunner


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subparser_name')
    parser_list = subparsers.add_parser('list', help='list datasets')
    parser_run = subparsers.add_parser('run', help='run analysis')
    parser_dataimport = subparsers.add_parser('dataimport', help='import dataset')
    parser_test = subparsers.add_parser('test')
    parser_add = subparsers.add_parser('query')
    parser_am = subparsers.add_parser('analysis')
    parser_aq = subparsers.add_parser('module_query')
    parser_pr = subparsers.add_parser('print')
    parser_r = subparsers.add_parser('relation')

    parser.add_argument('--config-file')
    parser.add_argument('--filename')
    parser.add_argument('--fileid')
    parser.add_argument('--featname')
    parser.add_argument('--parents')
    parser.add_argument('--params')
    parser.add_argument('--inputs')

    parser.add_argument('--description')
    parser.add_argument('--details')

    args = parser.parse_args()
    with open(args.config_file, 'r') as config_file:
        config = yaml.load(config_file)

    sr = SparkRunner(config)
    if args.subparser_name == 'list':
        sr.list_datasets()
    elif args.subparser_name == 'run':
        sr.run_analysis(args.params, args.inputs)
    elif args.subparser_name == 'dataimport':
        sr.create_dataset(args.inputs, args.description, args.details)
    elif args.subparser_name == 'test':
        print(args.filename)
        sr.test_insert(args.filename)
    elif args.subparser_name == 'relation':
        sr.create_relation(args.featname, args.parents)
    elif args.subparser_name == 'query':
        sr.test_query(args.fileid)
    elif args.subparser_name == 'analysis':
        sr.test_analysis()
    elif args.subparser_name == 'module_query':
        sr.query_analysis(args.fileid)
    elif args.subparser_name == 'print':
        s = args.fileid
        s = s.split(',')
        print(s)

if __name__ == '__main__':
    main()
