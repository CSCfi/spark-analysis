import argparse
from sparkles.modules.utils.runner import SparkRunner


def main():
    parser = argparse.ArgumentParser(description='Sparkles shell utility to list modules and datasets')
    subparsers = parser.add_subparsers(dest='subparser_name')
    parser_list = subparsers.add_parser('list_datasets', help='list datasets')
    parser_run = subparsers.add_parser('list_modules', help='list analysis modules')
    parser.add_argument('--prefix')
    parser.add_argument('--configfile', required=True)
    args = parser.parse_args()

    if(not args.configfile):
        raise RuntimeError('Configuration file for Sparkles is required')
    if(not args.prefix):
        args.prefix = ''

    sr = SparkRunner(args.configfile)

    if args.subparser_name == 'list_datasets':
        sr.list_datasets(args.prefix)
    elif args.subparser_name == 'list_modules':
        sr.list_modules(args.prefix)

# if __name__ == '__main__':
#    main()
