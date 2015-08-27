import unittest
from sparkles.modules.tests.runner_test import Runner_Test
from sparkles.modules.tests.dataframe_mock import DataframeMock
import sparkles.modules.tests.helper_test as helper_test
import json


class Interface_Tests(unittest.TestCase):

    def test_list_modules(self):

        """Used to test list_modules runnertion of the main interface.
        """
        runner_test = Runner_Test()
        self.assertEqual('EventCount', runner_test.list_modules('EventCo'))  # Search by prefix
        self.assertEqual('EventCount', runner_test.list_modules('Ev'))  # Same result by prefix
        self.assertEqual('Liquid', runner_test.list_modules('Liq'))
        self.assertEqual('EventCount,Liquid', runner_test.list_modules())  # List everything in the backend

    def test_list_datasets(self):

        """Used to test list_modules runnertion of the main interface.
        """
        runner_test = Runner_Test()

        # Datasets in the backend are (for demo) AB00, CD01 and one featureset FeatSet01
        self.assertEqual('AB00', runner_test.list_datasets('AB'))  # Search by prefix
        self.assertEqual('AB00', runner_test.list_datasets('A'))  # Same result by prefix
        self.assertEqual('CD01', runner_test.list_datasets('CD'))
        self.assertEqual('Featureset: FeatSet01', runner_test.list_datasets('Feat'))  # Featureset
        self.assertEqual('AB00,CD01,Featureset: FeatSet01', runner_test.list_datasets())  # List everything in the backend

    def test_import_analysis(self):

        """Used to test import_analysis runnertion of the main interface.
        Exception Testing also included
        """

        runner_test = Runner_Test()

        modulename = 'somemodule'
        filepath = '/path/to/module.py'
        params = 'starting, ending times, intervals....'
        inputs = 'parquet file with example attributes ...'
        outputs = 'featureset with columns .....'

        self.assertEqual('import_success', runner_test.mock_import_analysis(name=modulename, description='description', details='details', filepath=filepath, params=params, inputs=inputs, outputs=outputs))

        # Exception Tests when module already exists in backend
        modulename_exists = 'existing_module'
        self.assertRaises(RuntimeError, lambda: runner_test.mock_import_analysis(name=modulename_exists, description='description', details='details', filepath=filepath, params=params, inputs=inputs, outputs=outputs))

    def test_import_datasets(self):

        """Used to test import_dataset runnertion of the main interface.
        Exception Testing also included
        """

        runner_test = Runner_Test()
        inputfiles = ['/path/to/dataset.h5', '/path/to/anotherdataset.h5']
        userdatadir = 'swift://containerFeatures.SparkTest'  # The swift storage userdata directory

        self.assertEqual('import_success', runner_test.mock_import_dataset(inputfiles=inputfiles, description='something', details='some details', userdatadir=userdatadir))
        # Exceptions Tests
        # Missing parameters
        self.assertRaises(RuntimeError, lambda: runner_test.mock_import_dataset(inputfiles=inputfiles))
        self.assertRaises(RuntimeError, lambda: runner_test.mock_import_dataset(userdatadir=userdatadir))
        self.assertRaises(RuntimeError, lambda: runner_test.mock_import_dataset(inputfiles=inputfiles, description='something', details='something'))

    def test_run_analysis(self):

        """Used to test run_analysis runnertion of the main interface.
        Tests for both featureset and non-featureset options.
        Exception Testing also included
        """

        runner_test = Runner_Test()
        params = {}
        inputs_correct = ['existing_dataset', 'dataset']
        modulename_exists = 'existing_module'  # Name of a module which exists in backend

        # Test the Run with no featureset generation
        self.assertEqual(runner_test.mock_run_analysis(modulename=modulename_exists, params=params, inputs=inputs_correct), 'run_ok_nofeatureset')

        # Test the Run with featureset option
        features = {}
        self.assertEqual(runner_test.mock_run_analysis(modulename=modulename_exists, params=params, inputs=inputs_correct, features=features), 'run_ok_withfeatureset')

        # EXCEPTIONS TESTS
        # Exception when the module doesn't exist in the backend

        modulename_incorrect = 'wrongmodule'
        self.assertRaises(RuntimeError, lambda: runner_test.mock_run_analysis(modulename=modulename_incorrect, params=params, inputs=inputs_correct))

        # Exception when the datasets provided don't exist in the backend
        inputs_incorrect = ['dataset0', 'dataset00']
        self.assertRaises(RuntimeError, lambda: runner_test.mock_run_analysis(modulename=modulename_exists, params=params, inputs=inputs_incorrect))

        # Exceptions When not all the parameters are provided
        self.assertRaises(RuntimeError, lambda: runner_test.mock_run_analysis())
        self.assertRaises(RuntimeError, lambda: runner_test.mock_run_analysis(modulename=modulename_exists))
        self.assertRaises(RuntimeError, lambda: runner_test.mock_run_analysis(modulename=modulename_exists, params=params))
        self.assertRaises(RuntimeError, lambda: runner_test.mock_run_analysis(params=params, inputs=inputs_correct))
        self.assertRaises(RuntimeError, lambda: runner_test.mock_run_analysis(modulename=modulename_exists, inputs=inputs_correct))

    def test_save_datasets(self):

        """ This is to test the saving of a dataset when you run the import_datasets function of the main interface.
        Tries to save the metadata, and uploading of Parquet file to Swift. Contains Exceptions checks too.
        """

        dataframe = DataframeMock()  # The dataframe is generated manually in the analysis modules we write. Here we are mocking it

        configpath = '/path/to/config.yml'
        userdatadir = 'swift://containerFiles.SparkTest'
        originalpath = '/path/to/file.h5'
        self.assertTrue(helper_test.saveDataset(configpath, dataframe, userdatadir, "orders", originalpath, 'description', 'details'))

        # Exceptions
        invalid_userdatadir = 'swift://wrongContainer.SparkTest'
        self.assertRaises(RuntimeError, lambda: helper_test.saveDataset(configpath, dataframe, invalid_userdatadir, "orders", originalpath, 'description', 'details'))

        invalid_originalpath = 'wrong/path/or/filename.h5'
        self.assertRaises(RuntimeError, lambda: helper_test.saveDataset(configpath, dataframe, userdatadir, "orders", invalid_originalpath, 'description', 'details'))

    def test_save_featuresets(self):

        """ This is to test the saving of a featureset when you run the run_analysis function of the main interface.
        Tries to save the metadata, and uploades the featureset Parquet file to Swift. Also creates relations between datasets.
        """

        dataframe = DataframeMock()
        configpath = '/path/to/config.yml'
        userdatadir = 'swift://containerFeatures.SparkTest'

        featureset_name = 'feat'
        modulename = 'existing_module'
        module_parameters = [12345, 56789]
        parent_datasets = ['existing_dataset']

        self.assertTrue(helper_test.saveFeatures(configpath, dataframe, userdatadir, featureset_name, 'description', 'details', modulename, json.dumps(module_parameters), json.dumps(parent_datasets)))

        # Exceptions

        # Error when trying to save in a wrong container
        invalid_userdatadir = 'swift://wrongContainer.SparkTest'
        self.assertRaises(RuntimeError, lambda: helper_test.saveFeatures(configpath, dataframe, invalid_userdatadir, featureset_name, 'description', 'details', modulename, json.dumps(module_parameters), json.dumps(parent_datasets)))

        # Error when trying to save featureset with same name
        existing_featureset = 'existing_feat'
        self.assertRaises(RuntimeError, lambda: helper_test.saveFeatures(configpath, dataframe, userdatadir, existing_featureset, 'description', 'details', modulename, json.dumps(module_parameters), json.dumps(parent_datasets)))

        # Error when trying to use a module which does not exist
        nonexisting_module = 'wrong_module'
        self.assertRaises(RuntimeError, lambda: helper_test.saveFeatures(configpath, dataframe, userdatadir, featureset_name, 'description', 'details', nonexisting_module, json.dumps(module_parameters), json.dumps(parent_datasets)))
