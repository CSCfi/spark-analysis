import unittest
from sparkles.modules.tests.func_test import Functions_Test


class Interface_Tests(unittest.TestCase):

    def test_list_modules(self):

        """Used to test list_modules function of the main interface.
        """
        func_test = Functions_Test()
        self.assertEqual('EventCount', func_test.list_modules('EventCo'))  # Search by prefix
        self.assertEqual('EventCount', func_test.list_modules('Ev'))  # Same result by prefix
        self.assertEqual('Liquid', func_test.list_modules('Liq'))
        self.assertEqual('EventCount,Liquid', func_test.list_modules())  # List everything in the backend

    def test_list_datasets(self):

        """Used to test list_modules function of the main interface.
        """
        func_test = Functions_Test()

        self.assertEqual('AB00', func_test.list_datasets('AB'))  # Search by prefix
        self.assertEqual('AB00', func_test.list_datasets('A'))  # Same result by prefix
        self.assertEqual('CD01', func_test.list_datasets('CD'))
        self.assertEqual('Featureset: FeatSet01', func_test.list_datasets('Feat'))  # Featureset
        self.assertEqual('AB00,CD01,Featureset: FeatSet01', func_test.list_datasets())  # List everything in the backend

    def test_import_analysis(self):

        """Used to test import_analysis function of the main interface.
        Exception Testing also included
        """

        func_test = Functions_Test()

        modulename = 'somemodule'
        filepath = '/path/to/module.py'
        params = 'starting, ending times, intervals....'
        inputs = 'parquet file with example attributes ...'
        outputs = 'featureset with columns .....'

        self.assertEqual('import_success', func_test.mock_import_analysis(name=modulename, description='description', details='details', filepath=filepath, params=params, inputs=inputs, outputs=outputs))

        # Exception Tests when module already exists in backend
        modulename_exists = 'existing'
        self.assertRaises(RuntimeError, lambda: func_test.mock_import_analysis(name=modulename_exists, description='description', details='details', filepath=filepath, params=params, inputs=inputs, outputs=outputs))

    def test_import_datasets(self):

        """Used to test import_dataset function of the main interface.
        Exception Testing also included
        """

        func_test = Functions_Test()
        inputfiles = ['/path/to/dataset.h5', '/path/to/anotherdataset.h5']
        userdatadir = 'swift://containerFeatures.SparkTest'  # The swift storage userdata directory

        self.assertEqual('import_success', func_test.mock_import_dataset(inputfiles=inputfiles, description='something', details='some details', userdatadir=userdatadir))
        # Exceptions Tests
        # Missing parameters
        self.assertRaises(RuntimeError, lambda: func_test.mock_import_dataset(inputfiles=inputfiles))
        self.assertRaises(RuntimeError, lambda: func_test.mock_import_dataset(userdatadir=userdatadir))
        self.assertRaises(RuntimeError, lambda: func_test.mock_import_dataset(inputfiles=inputfiles, description='something', details='something'))

    def test_run_analysis(self):

        """Used to test run_analysis function of the main interface.
        Tests for both featureset and non-featureset options.
        Exception Testing also included
        """

        func_test = Functions_Test()
        params = {}
        inputs_correct = ['dataset', 'dataset']
        modulename_exists = 'existing'  # Name of a module which exists in backend

        # Test the Run with no featureset generation
        self.assertEqual(func_test.mock_run_analysis(modulename=modulename_exists, params=params, inputs=inputs_correct), 'run_ok_nofeatureset')

        # Test the Run with featureset option
        features = {}
        self.assertEqual(func_test.mock_run_analysis(modulename=modulename_exists, params=params, inputs=inputs_correct, features=features), 'run_ok_withfeatureset')

        # EXCEPTIONS TESTS
        # Exception when the module doesn't exist in the backend

        modulename_incorrect = 'wrongmodule'
        self.assertRaises(RuntimeError, lambda: func_test.mock_run_analysis(modulename=modulename_incorrect, params=params, inputs=inputs_correct))

        # Exception when the datasets provided don't exist in the backend
        inputs_incorrect = ['dataset0', 'dataset00']
        self.assertRaises(RuntimeError, lambda: func_test.mock_run_analysis(modulename=modulename_exists, params=params, inputs=inputs_incorrect))

        # Exceptions When not all the parameters are provided
        self.assertRaises(RuntimeError, lambda: func_test.mock_run_analysis())
        self.assertRaises(RuntimeError, lambda: func_test.mock_run_analysis(modulename=modulename_exists))
        self.assertRaises(RuntimeError, lambda: func_test.mock_run_analysis(modulename=modulename_exists, params=params))
        self.assertRaises(RuntimeError, lambda: func_test.mock_run_analysis(params=params, inputs=inputs_correct))
        self.assertRaises(RuntimeError, lambda: func_test.mock_run_analysis(modulename=modulename_exists, inputs=inputs_correct))
