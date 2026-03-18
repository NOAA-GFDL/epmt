"""refs.py
handles reference models
"""

from logging import getLogger
from json import dumps
import pandas as pd
from .dash_config import MOCK_EPMT_API

if MOCK_EPMT_API:
    from . import epmt_query_mock as eq
else:
    from epmt import epmt_query as eq

# We log how we want
# pylint: disable=invalid-name, logging-format-interpolation
logger = getLogger(__name__)


def get_refs():
    """
    Returns list of references for displaying in table
    """
    m = eq.get_refmodels()
    return [[nm['id'], nm['name'], nm['created_at'], nm['tags'], nm['jobs'],
             ['duration', 'cpu_time', 'num_procs'], nm['enabled']] for nm in m]


def make_refs(name='', jobs=None, tags=None, active=True):
    """trash, duplicate unnecissary
    """
    # eq.create_refmodel(jobs=['625133','693118','696085'],
    # name='Sample', tag={'exp_name':'ESM4_historical_D151','exp_component': 'atmos_cmip'})
    nm = eq.create_refmodel(jobs=jobs, name=name, tag=tags, enabled=active)
    # logger.error("Create model failed {}".format(e))
    # return None
    return [[nm['id'], nm['name'], nm['created_at'], nm['tags'], nm['jobs'],
             ['duration', 'cpu_time', 'num_procs'], nm['enabled']]]


class ref_gen:
    """Generate a list of sample references
    ref_gen does data cleanup and conversions for displaying reference models"""

    def __init__(self):
        # references = make_refs(2)
        self.df = pd.DataFrame(get_refs(), columns=['id',
                                                    'name', 'date created', 'tags', 'jobs',
                                                    'features', 'active'])
        # self.df['active'] = np.where(self.df['active'], 'Yes', 'No')
        # Reorder
        self.df = self.df[['id', 'name', 'active',
                           'date created', 'tags', 'jobs', 'features']]


def get_references():
    """
    Grabs sample reference models
    formats them
    return: formatted dataframe
    """
    models = ref_gen().df
    # Ref model initialization data
    models['tags'] = models['tags'].apply(dumps)  # Dumps stringify's dictionaries
    models['jobs'] = models['jobs'].apply(dumps)  # Dumps stringify's lists
    models['features'] = models['features'].apply(
        dumps)  # Dumps stringify's lists
    return models


def edit_model(model_name, new_jobs, del_original=True):
    """
    Accepts primary key model_name and new jobs
    Deletes original model then creates new one
    Returns ID of new model
    """
    orig_model = eq.get_refmodels(model_name)[0]
    mname = orig_model['name']
    mid = orig_model['id']
    mtags = orig_model['tags']
    menabled = orig_model['enabled']
    mjobs = new_jobs

    if del_original:
        eq.delete_refmodels(mid)
    ret = make_refs(name=mname, jobs=mjobs, tags=mtags, active=menabled)
    return ret[0][0]


ref_df = get_references()
