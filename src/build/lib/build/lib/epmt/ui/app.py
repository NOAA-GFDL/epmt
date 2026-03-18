""" app maintains server configurations
bootstrap and css are included here
"""

from logging import getLogger
import dash_bootstrap_components as dbc
import dash

# We log how we want
# pylint: disable=invalid-name, logging-format-interpolation
logger = getLogger(__name__)  # you can use other name
logger.propagate = False


external_stylesheets = [  # Remote Styles
    dbc.themes.BOOTSTRAP,
    "https://cdnjs.cloudflare.com/ajax/libs/normalize/7.0.0/normalize.min.css",
    "https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",

    # Fonts
    "https://fonts.googleapis.com/css?family=Raleway:400,300,600",
    "https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css",

    # Locally saved /assets/*
    # "https://codepen.io/bcd/pen/KQrXdb.css", # Bordering & Theming
    # "https://codepen.io/dmcomfort/pen/JzdzEZ.css", # Data table theming
    # 'https://codepen.io/chriddyp/pen/bWLwgP.css', # Plotly Css
]

app = dash.Dash(
    __name__, external_stylesheets=external_stylesheets, url_base_pathname='/')
server = app.server
app.config.suppress_callback_exceptions = True
fullurl = ''
# app.css.config.serve_locally = False
# app.scripts.config.serve_locally = False

# import dash_auth

# VALID_USERNAME_PASSWORD_PAIRS = [
#     ['alg', 'mexicovacation']
# ]

# auth = dash_auth.BasicAuth(
#     app,
#     VALID_USERNAME_PASSWORD_PAIRS
# )
