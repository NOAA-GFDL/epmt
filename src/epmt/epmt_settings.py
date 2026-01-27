"""
EPMT settings module - loads default settings and user-specific overrides.
"""
# load defaults
from epmt.epmt_default_settings import *

# now load the user-specific settings.py so they override the defaults
# if you want your own configuraions, put it in settings.py, not here
try:
    from epmt.settings import *
except Exception as e:
    raise ModuleNotFoundError('alternate epmt.settings import approach did not' +
                              ' work and neither did the first attempt!') from e
