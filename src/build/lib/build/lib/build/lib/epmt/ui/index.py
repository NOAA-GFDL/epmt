"""index.py
Configure the Dash server and load the layout

This file loads the initial layout as well as handles custom url query page delivery.
"""

# import dash_core_components as dcc
from dash import dcc

# import dash_html_components as html
from dash import html

from dash.dependencies import Input, Output
from .app import app
from . import layouts as lay
from . import callbacks  
from logging import getLogger, basicConfig, DEBUG, ERROR, INFO, WARNING
logger = getLogger(__name__)


def init_app():
    """
    Configure initial layout
    """
    # see https://dash.plot.ly/external-resources to alter header, footer and favicon
    app.index_string = '''
    <!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>EPMT Job Display</title>
            {%favicon%}
            {%css%}
        </head>
        <body>
            {%app_entry%}
            <footer>
                {%config%}
                {%scripts%}
                {%renderer%}
            </footer>
        </body>
    </html>
    '''
    # <img src="\\assets\\cc_logo.jpeg" width="120" height="120">

    app.layout = html.Div([
        dcc.Location(id='url', refresh=False),
        html.Div(id='page-content')
    ])

# Update page
# # # # # # # # #


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname'),
               Input('url', 'href')])
def display_page(pathname, pfullurl):
    """Method:
    For displaying/returning all layouts as requested by url
    """
    logger.info("Page requested {}".format(pfullurl))
    app.fullurl = pfullurl
    if pathname in ['', '/']:
        return lay.recent_jobs_page
    elif pathname == '/unprocessed/':
        return lay.layout_unprocessed
    elif pathname == '/alerts/':
        return lay.layout_alerts
    elif pathname == '/refs/':
        return lay.layout_references
    elif pathname == '/jobs':
        return lay.layouts(pfullurl)
    elif str(pathname).startswith('/graph'):
        return lay.graph_plotly(pfullurl)
    elif str(pathname).startswith('/compare'):
        return lay.compare(pfullurl)
    elif pathname == '/multilayout':
        return lay.multi_flow(pfullurl)
    else:
        return lay.noPage


if __name__ == '__main__':
    logger = getLogger(__name__)
    basicConfig(level=DEBUG)
    init_app()
    app.run_server(debug=True, host='0.0.0.0')
