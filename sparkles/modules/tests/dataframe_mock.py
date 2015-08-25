class DataframeMock(object):

    def __init__(self):
        pass

    def dtypes(self):
        return ['field1, field2']

    def saveAsParquetFile(self, tablepath):
        if(tablepath not in ['swift://containerFiles.SparkTest/file/file_orders.parquet', 'swift://containerFeatures.SparkTest/feat.parquet']):
            raise RuntimeError('table path error')
        return 'success'
