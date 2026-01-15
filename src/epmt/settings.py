"""
EPMT settings module - user configuration settings.
"""
def test_settings_import():
    pass

epmt_settings_kind = 'empty'
verbose = 2
max_log_statement_length = pow(2, 11)  # max number of list elements to print for long sql queries

db_params = {'url': 'postgresql://epmt:Goal2020@workflow1:5432/epmt', 'echo': False}
