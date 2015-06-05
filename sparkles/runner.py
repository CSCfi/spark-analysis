from sparkles.models import Base, config_to_db_session


class SparkRunner(object):

    def __init__(self, config):
        print(config)
        self.session = config_to_db_session(config, Base)

    def list_analysises(self):
        raise NotImplementedError

    def list_datasets(self):
        raise NotImplementedError

    def run_analysis(self, *args, **kwargs):
        raise NotImplementedError
