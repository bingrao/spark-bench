from argparse import ArgumentParser


def config_opts(parser):
    parser.add_argument('-project_dir', '--project_dir', required=True, type=str, default='')
    parser.add_argument('-data', '--data', type=str, default="data/")
    parser.add_argument('-config', '--config', required=False, help='Config file path')
    parser.add_argument('-project_log', '--project_log', type=str, default='')
    parser.add_argument('-debug', '--debug', type=bool, default=False)


def get_default_argument(desc='default'):
    parser = ArgumentParser(description=desc)
    config_opts(parser)
    args = parser.parse_args()
    config = vars(args)
    return config
