from sparkles.modules.utils.models import Analysis, Dataset
from mock import Mock


def list_mod_se(*args, **kwargs):

    mod1 = Mock(spec=Analysis)
    mod1.name = 'EventCount'
    mod2 = Mock(spec=Analysis)
    mod2.name = 'Liquid'
    if(args[0] in ('E', 'Ev', 'Eve', 'Event', 'EventC', 'EventCo', 'EventCou', 'EventCount')):
        return [mod1]
    elif(args[0] in ('L', 'Li', 'Liq', 'Liqu', 'Liqui', 'Liquid')):
        return [mod2]
    elif(args[0] == ''):
        return [mod1, mod2]
    else:
        return []


def list_ds_se(*args, **kwargs):

    ds1 = Mock(spec=Dataset)
    ds1.name = 'AB00'
    ds1.module = None
    ds2 = Mock(spec=Dataset)
    ds2.name = 'CD01'
    ds2.module = None

    ds = Mock(spec=Dataset)
    ds.name = 'FeatSet01'
    ds.module = 'module'

    if(args[0] in ('AB', 'A')):
        return [ds1]
    elif(args[0] in ('CD', 'C')):
        return [ds2]
    elif(args[0] == ''):
        return [ds1, ds2, ds]
    elif(args[0] == 'Feat'):
        return [ds]
    else:
        return []


def call_se(*args, **kwargs):

    if(len(args[0]) == 7):
        return 'run_ok_nofeatureset'
    else:
        return 'run_ok_withfeatureset'


def ds_se(*args, **kwargs):
    if kwargs['name'] in ['dataset', 'existing_dataset']:
        ds = Mock(spec=Dataset)
        ds.filepath = '/path/to/dataset'
        ds.id = '00000abc'
        return ds
    else:
        return None


def feat_se(*args, **kwargs):

    if kwargs['name'] == 'existing_feat':
        feat = Mock(spec=Dataset)
        feat.id = '1234xyz'
        return feat
    else:
        return None


def relation_se(*args, **kwargs):

    '''This side effect mocks the effect of a database table with a newly added featureset "feat" for use in create_relation
    '''
    if kwargs['name'] in ['existing_feat', 'feat']:
        feat = Mock(spec=Dataset)
        feat.id = '1234xyz'
        return feat
    else:
        return None


def mod_se(*args, **kwargs):
    if kwargs['name'] == 'existing_module':

        amod = Mock(spec=Analysis)
        amod.id = 'xyz0000abc'
        amod.filepath = '/path/to/module'
        return amod
    else:
        return None
