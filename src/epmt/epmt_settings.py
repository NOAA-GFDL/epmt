"""
EPMT settings module - loads default settings and user-specific overrides.
"""
# load defaults
from epmt.epmt_default_settings import *

# now load the user-specific settings.py so they override the defaults
# if you want your own configurations, put it in settings.py, not here
from epmt.settings import *
