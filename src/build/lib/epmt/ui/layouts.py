"""Layouts.py
These are the dash templates for each page that will be displayed
"""

# pylint: disable=import-error
from logging import getLogger, basicConfig, DEBUG
from datetime import datetime as dt
from json import dumps
import dash_bootstrap_components as dbc
import dash_daq as daq
# import dash_core_components as dcc
from dash import dcc
# import dash_html_components as html
from dash import html
from dash import dash_table

from .dash_config import DEFAULT_ROWS_PER_PAGE
from .refs import ref_df
from .jobs import JobGen
from .components import Header, Footer, parse_url
from .graphing import graph_experiment, graph_ops, graph_components, bar_graph, create_gantt_graph, create_boxplot, create_grouped_bargraph
logger = getLogger(__name__)  # pylint: disable=invalid-name
# basicConfig(level=DEBUG)


def create_conditional_style(dataframe):
    """Autosizing Columns
    """
    pixel_for_char = 10
    style = []
    for col in dataframe.columns:
        name_length = len(col)
        pixel = 50 + round(name_length * pixel_for_char)
        pixel = str(pixel) + "px"
        style.append({'if': {'column_id': col}, 'minWidth': pixel})
    return style


recent_jobs_page = html.Div([
    html.Div([
        # CC Header
        html.Div(style={'inline': 'true'}, children=[
            Header(),
        ]),
    ]),
    # These tabs are huge
    # https://community.plot.ly/t/adjusting-height-of-tabs/13136/5
    dcc.Tabs(id="tabs", value='jobs', children=[
        dcc.Tab(label='Recent Jobs', value='jobs', children=[
                dbc.Container([
                    dbc.Row(
                        [
                            dbc.Col(
                                ["Search:",
                                 dcc.Input(
                                     id='searchdf',
                                     placeholder='(job id, component, exp name, tags)',
                                     type='text',
                                     value='',
                                     style={'display': 'block',
                                            'width': '100%'},
                                     # persistence=True,
                                     # persistence_type='memory'
                                 )],
                                width="auto",
                                # md=3,
                                lg=4
                            ),
                            html.Div(id='switches',
                                     children=[
                                         dbc.Col("Raw Data"),
                                         dbc.Col(
                                             daq.ToggleSwitch(
                                                 id='raw-switch',
                                                 # label='Raw Data',
                                                 # labelPosition='left',
                                                 # style={'display':'inline-block','fontsize':'medium'}, # Set font size
                                                 # so it's not randomly inherited between browsers
                                                 value=False,
                                                 color='Green'
                                             ),
                                             width="auto"
                                         )
                                     ]
                                     ),
                        ],
                        justify="between",
                        align="center"
                    ),

                ], fluid=True),

                # First Data Table
                html.Div([
                    dash_table.DataTable(
                        id='table-multicol-sorting',
                        row_selectable="multi",
                        page_current=0,
                        page_size=DEFAULT_ROWS_PER_PAGE,
                        page_action='custom',
                        sort_action='custom',
                        sort_by=[],
                        # sort_mode='multi', #Keeping it simple now
                        # data=df.head(10).to_dict('records'), # Do not display data initially, callback will handle it
                        # filter_action="native",
                        # style_as_list_view=True,
                        columns=[
                            {"name": i, "id": i} for i in JobGen().jobs_df.columns
                        ],
                        fixed_rows={'headers': True, 'data': 0},
                        # fixed_columns={ 'headers': True, 'data': 1 },#, Css is not setup for this
                        style_table={
                            'padding': '5px',
                            # 'height': '430px',
                            'font-size': '14px'
                        },
                        style_header={
                            'font-weight': 'bold',
                            'padding': '5px',
                            'whiteSpace': 'normal',
                            # 'overflow': 'visible',
                            # 'font-size':'14px',
                        },
                        style_cell={
                            'font-family': 'sans-serif',
                            'overflow': 'hidden',
                            'minWidth': '100px',
                            # 'font-size':'14px',
                            # 'textOverflow': 'ellipsis',
                        },
                        style_header_conditional=[
                            {
                                'if': {'column_id': 'tags'},
                                'text-align': 'left',
                            },
                            {
                                'if': {'column_id': 'job id'},
                                'text-align': 'right',
                            },

                        ],
                        style_data_conditional=[],
                    ),
                    html.Div([
                        dbc.Row([
                            dbc.Col([
                                dbc.Row([
                                    dbc.Alert(
                                        children="",
                                        id="run-create-alert",
                                        is_open=False,
                                        dismissable=True,
                                    ),
                                ]),
                                dbc.Row([
                                    html.Div(id='name-model-div', style={'display': 'none'}, children=[
                                        # Containers have nice margins and internal spacing
                                        dbc.Container([
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        # model name input
                                                        # dbc.FormGroup(
                                                        dbc.Row(
                                                            [
                                                                dbc.Label("Model"),
                                                                dbc.Input(
                                                                    id='model-name-input',
                                                                    placeholder="model name here", type="text"),
                                                                dbc.FormText(
                                                                    "Enter a Reference Model Name"),
                                                            ]
                                                        ),
                                                        width="auto"

                                                    ),
                                                    dbc.Col([
                                                        # Button for save
                                                        html.Button(id='save-newModel-btn',
                                                            children='Save Model', n_clicks_timestamp=0),
                                                        # Button for close
                                                        html.Button(id='create-Model-close-btn', children='Close', n_clicks_timestamp=0)],
                                                        width=3,
                                                        align="center"
                                                    ),

                                                    # Models created notification
                                                    dbc.Col([html.Div(style={'inline': 'true'}, children=["Model Status:",
                                                                                                          html.Div(id='recent-job-model-status',
                                                                                                                   children='')
                                                                                                          ])])

                                                ]
                                            )
                                        ], fluid=True),
                                    ]),
                                ]),
                                dbc.Row([
                                    dbc.Col([
                                        html.Button(id='run-analysis-btn', children="Run Analysis", n_clicks_timestamp=0,
                                            style={'background-color': '#20c997', 'color': '#020080'}),
                                    ], width='auto'),
                                    dbc.Col([
                                        html.Button(id='create-newModel-btn',
                                            children="Create Model from Selected Jobs", n_clicks_timestamp=0),
                                    ], width='auto'),
                                    dbc.Col([
                                        html.Button(id='index-select-all',
                                            children="Select All"),
                                    ], width='auto'),
                                    dbc.Col([
                                        html.Div(style={'display': 'block', 'width': '360px', 'text-align': 'center'}, children=[
                                            dcc.DatePickerRange(
                                                id='jobs-date-picker',
                                                min_date_allowed=dt(1990, 1, 1),
                                                max_date_allowed=dt(2040, 12, 25),
                                                initial_visible_month=dt(2019, 6, 5),
                                                clearable=True,
                                                with_portal=True,
                                                show_outside_days=True,
                                                minimum_nights=0
                                            ), "(Inclusive Date Selections)"]),
                                    ], width='auto'),
                                ]),
                                dbc.Row([
                                    # Selected jobs notification
                                    dbc.Col([
                                        html.Div(children=[
                                            "Available test Models: ",
                                            dbc.Col(
                                                dcc.Dropdown(
                                                    id='model-selector-dropdown',
                                                    options=[
                                                        {'label': "No Model",
                                                         'value': "None"}
                                                    ],
                                                    value="None",
                                                    # style={'display': 'block', 'width': '100%'}
                                                ), width="11"),
                                            dbc.Col([
                                                dcc.Dropdown(
                                                    id='row-count-dropdown',
                                                    options=[
                                                        {'label': '5 Rows', 'value': '5'},
                                                        {'label': '30 Rows',
                                                         'value': '30'},
                                                        {'label': '50 Rows',
                                                         'value': '50'},
                                                        {'label': '1000 Rows',
                                                         'value': '1000'}
                                                    ],
                                                    clearable=False,
                                                    searchable=False,
                                                    value=DEFAULT_ROWS_PER_PAGE,
                                                    persistence=True,
                                                    persistence_type='local'
                                                )
                                            ], width=5),
                                            # df.shape[0]
                                            # Old Page attempt
                                            # dbc.Col(['Page:'], html.Div(id="page-selector", children=[dcc.Link(str(n+1)+", ",href="?page="+str(n)) for n in range((job_df.shape[0]//DEFAULT_ROWS_PER_PAGE))])
                                            # , width='auto'),
                                            # ','.join([str(n+1) for n in range((job_df.shape[0]//DEFAULT_ROWS_PER_PAGE))]),
                                            dbc.Col([
                                                "[ ",
                                                JobGen().jobs_df.shape[0],
                                                " Jobs Total ]"
                                            ], width='auto'),
                                        ])
                                    ]),
                                ]),
                            ], width=10),
                            dbc.Col([html.Div(children=[
                                "Quick Links Here"
                            ], id='quick-links', style={'display': 'none'})])
                        ])
                    ])


                ], className="subpage"),
                ]),
        # Reference model datatable tab
        dcc.Tab(label='Models', value='models', children=[
            dbc.Modal(
                [
                    dbc.ModalHeader("Header"),
                    dbc.ModalBody("This is the content of the modal"),
                    dbc.ModalFooter(
                        dbc.Button("Close", id="close", className="ml-auto")
                    ),
                ],
                id="modal",
            ),
            html.Div([
                html.H6(["Reference Models"],
                        className="gs-header gs-text-header padded", style={'marginTop': 15})
            ]),
            # Radio Button

            html.Div([
                # The inline dropdowns are broken[not displayed] due to my sorting css work on column headers
                dash_table.DataTable(
                    id='table-ref-models',
                    row_selectable="single",
                    sort_action='native',
                    page_action='native',
                    page_current=0,
                    page_size=6,
                    # sort_mode='multi', Keeping it simple now
                    # data=df.head(10).to_dict('records'), # Do not display data initially, callback will handle it
                    # filter_action="native",
                    # style_as_list_view=True,
                    columns=[
                        {"name": i, "id": i} for i in ref_df.columns
                        # {"name":"Model","id":"Model"},
                        # {"name":"Active","id":"Active"},#,"presentation":"dropdown"},
                        # {"name":"Tags","id":"Tags"},
                        # {"name":"Jobs","id":"Jobs"},
                        # {"name":"Features","id":"Features"},
                    ],
                    style_table={
                        'padding': '5px',
                        'overflowX': 'scroll'
                    },
                    data=ref_df.to_dict('records'),
                    # editable=True,
                    dropdown={
                        'Active': {
                            'options': [
                                {'label': i, 'value': i}
                                for i in ['True', 'False']
                            ]
                        }
                    },
                    fixed_rows={'headers': True, 'data': 0},
                    # fixed_columns={ 'headers': True, 'data': 1 },#, Css is not setup for this
                    style_header={
                        # 'overflow': 'visible',
                        'font-size': '19px',
                        'font-weight': 'bold',
                        'padding': '10px',
                        'whiteSpace': 'normal',
                        # 'width':'90px',
                        # 'height':'40px'
                        # 'text-align':'center',
                    },
                    style_cell={
                        'font-family': 'sans-serif',
                        # 'font-size':'16px',
                        # 'overflow': 'hidden',
                        'minWidth': '70px',  # , 'maxWidth': '140px',
                        'height': '50px'
                    },
                    # For some reason {active} != True or true or 0 wouldn't work
                    # Color all data rows pink then color good rows white
                    style_data_conditional=[
                        {'if': {'filter_query': '{active} > 0'},
                         'backgroundColor': '#ffffff'
                         },
                        # Shrink Narrow columns
                        {
                            'if': {'column_id': 'jobs'},
                            'minWidth': '180px',
                        },
                    ],
                    style_data={'backgroundColor': '#FFc0b5',
                                'whiteSpace': 'normal',
                                'minWidth': '0px', 'maxWidth': '180px',
                                'height': 'auto'},
                ),
                html.Div(id='edit-model-div', style={'display': 'contents'}, children=[
                    # Containers have nice margins and internal spacing
                    dbc.Container([
                        dbc.Row(
                            [
                                dbc.Col(
                                    # Dropdown with jobs populated by callback
                                    dcc.Dropdown(
                                        multi=True,
                                        id='edit-model-jobs-drdn',
                                        options=[
                                            {'label': 'Job0', 'value': 'j0'},
                                            {'label': 'Job1', 'value': 'j1'}
                                        ],
                                        value=['j0', 'j1'],
                                    )
                                ),
                                dbc.Col([
                                    # Button for save
                                    html.Button(id='edit-Model-save-btn',
                                                children='Save Model', n_clicks_timestamp=0),
                                    # Button for close
                                    html.Button(id='edit-Model-close-btn', children='Close', n_clicks_timestamp=0)]
                                )
                            ]
                        )
                    ], fluid=True),
                ]),
                html.Button(id='toggle-Model-btn',
                            children="Toggle Model Status", n_clicks_timestamp=0),
                html.Button(id='edit-Model-btn',
                            children="Edit Reference Model", n_clicks_timestamp=0),
                html.Button(id='delete-Model-btn', children="Delete Model", n_clicks_timestamp=0,
                            style={'background-color': '#ff0000', 'color': '#000000'}),
                html.Div(style={'display': 'none'}, id='placeholderedit'),
            ]),


        ])
    ]),
    Footer()
], className="page")

######################## END index Layout ########################


# logger.info(unproc)
######################## START unprocessed Layout ########################
layout_unprocessed = html.Div([
    html.Div([
        # CC Header
        Header(),
        # Date Picker

        # Header Bar
        html.Div([
            html.H6(["Unprocessed Jobs"],
                    className="gs-header gs-text-header padded", style={'marginTop': 15})
        ]),
        # Radio Button

        # First Data Table
        html.Div([
            dash_table.DataTable(
                id='table-multicol-sorting',
                columns=[
                    {"name": i, "id": i} for i in sorted(JobGen().jobs_df.columns)
                ],

            )
        ]),
        # GRAPHS
        html.Div([
            html.Div(
                id='update_graph_1'
            ),
            html.Div([
                html.P("Unprocessed Table Here")
            ]
            )]
        ),
    ], className="subpage")
], className="page")

######################## END unprocessed Layout ########################

######################## START References Layout ########################
layout_references = html.Div([
    html.Div([
        # CC Header
        Header(),
        # Date Picker

        # Header Bar
        html.Div([
            html.H6(["Reference Models"],
                    className="gs-header gs-text-header padded", style={'marginTop': 15})
        ]),
        # Radio Button

        # First Data Table
        html.Div([
            dash_table.DataTable(
                id='table-ref-models',
                row_selectable="multi",
                sort_action='native',
                # sort_mode='multi', Keeping it simple now
                # data=df.head(10).to_dict('records'), # Do not display data initially, callback will handle it
                # filter_action="native",
                # style_as_list_view=True,
                columns=[
                    # {"name": i, "id": i, "presentation":"dropdown"} for i in ref_df.columns
                    {"name": "Model", "id": "Model"},
                    {"name": "Active", "id": "Active", "presentation": "dropdown"},
                    {"name": "Tags", "id": "Tags"},
                    {"name": "Jobs", "id": "Jobs"},
                    {"name": "Features", "id": "Features"},
                ],
                data=ref_df.to_dict('records'),
                editable=True,
                dropdown={
                    'Active': {
                        'options': [
                            {'label': i, 'value': i}
                            for i in ['True', 'False']
                        ]
                    }
                },
                fixed_rows={'headers': True, 'data': 0},
                # fixed_columns={ 'headers': True, 'data': 1 },#, Css is not setup for this
                style_header={
                    # 'overflow': 'visible',
                    'font-size': '19px',
                    'font-weight': 'bold',
                    'padding': '10px',
                    'whiteSpace': 'normal',
                    # 'width':'90px',
                    # 'height':'40px'
                    # 'text-align':'center',
                },
                style_cell={
                    'font-family': 'sans-serif',
                    # 'font-size':'16px',
                    # 'overflow': 'hidden',
                    'minWidth': '70px',  # , 'maxWidth': '140px',
                    'height': '50px'
                },
                style_table={
                    'padding': '5px',
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'job id'},
                        'text-align': 'right',
                    }
                ],
                style_data_conditional=[
                    {
                        'if': {'column_id': 'job id'},
                        'text-align': 'right',
                    }
                ],
            )
        ]),

    ], className="subpage")
], className="page")
######################## END References Layout ########################


######################## START Display Layout ########################
layout_display = html.Div([
    html.Div([
        # CC Header
        Header(),
        # Date Picker

        # Header Bar
        html.Div([
            html.H6(["Layout Display"],
                    className="gs-header gs-text-header padded", style={'marginTop': 15})
        ]),
        # Radio Button

        # First Data Table
        html.Div([
            dash_table.DataTable(
                id='table-multicol-sorting',
                columns=[
                    {"name": i, "id": i} for i in sorted(JobGen().jobs_df.columns)
                ],
                data=JobGen().jobs_df.to_dict('records')
            )
        ]),
        # Download Button
        html.Div([
            html.A(html.Button('Download Data', id='download-button'),
                   id='download-link-ga-category')
        ]),
        # Second Data Table

        # GRAPHS
        html.Div([
            html.Div(
                id='update_graph_1'
            ),
            html.Div([
                html.P("Graph Here")
            ]
            )]
        ),
    ], className="subpage")
], className="page")

######################## END Display Layout ########################

######################## START alerts Layout ########################
layout_alerts = html.Div([
    html.Div([
        # CC Header
        Header(),
        # Date Picker

        # Header Bar
        html.Div([
            html.H6(["Alert Jobs"], className="gs-header gs-text-header padded",
                    style={'marginTop': 15})
        ]),
        # Radio Button

        # First Data Table
        html.Div([
            dash_table.DataTable(
                id='table-multicol-sorting',
                columns=[
                    {"name": i, "id": i} for i in sorted(JobGen().jobs_df.columns)
                ],
                data=JobGen().jobs_df.to_dict('records')
            )
        ]),
        # Download Button
        html.Div([
            html.A(html.Button('Download Data', id='download-button'),
                   id='download-link-ga-category')
        ]),
        # Second Data Table

        # GRAPHS
        html.Div([
            html.Div(
                id='update_graph_1'
            ),
            html.Div([
                html.P("Alert Table Here")
            ]
            )]
        ),
    ], className="subpage")
], className="page")


######################## END alerts Layout ########################

######################## START table Layout ########################
layout_sample = html.Div([
    html.Div([
        # dcc.Location(id='url', refresh=False),
        html.Div([
            dash_table.DataTable(
                id='custom-table',
                columns=[
                    {"name": i, "id": i} for i in sorted(JobGen().jobs_df.columns)
                ],
                # data=df.to_dict('records')
            )
        ])
    ], className="subpage")
], className="page")


def layouts(pfullurl):
    # offset = page * DEFAULT_ROWS_PER_PAGE
    q = parseurl(pfullurl)
    # Grab jobid values from query dict
    page = q['jobid'][:]
    # , limit=DEFAULT_ROWS_PER_PAGE, offset=page*DEFAULT_ROWS_PER_PAGE
    job_df = JobGen(jobs=page).jobs_df
    jobids = q.get('jobid', None)
    table_data = job_df
    if jobids:
        if job_df['job id'].isin(jobids).any():
            logger.debug("We have that job")
            table_data = job_df.loc[job_df['job id'].isin(jobids)]
            table_data['tags'] = table_data['tags'].apply(dumps)

    return html.Div([
        html.Div([
            # dcc.Location(id='url', refresh=False),
            html.Div([
                dash_table.DataTable(
                    id='custom-table',
                    columns=[
                        {"name": i, "id": i} for i in sorted(table_data.columns)
                    ],
                    sort_action='native',
                    data=table_data.to_dict('records'),
                    fixed_rows={'headers': True, 'data': 0},
                    # fixed_columns={ 'headers': True, 'data': 1 },#, Css is not setup for this
                    style_table={
                        'padding': '5px',
                        'height': '300px',
                        'font-size': '14px'
                    },
                    style_header={
                        'font-weight': 'bold',
                        'padding': '5px',
                        'whiteSpace': 'normal',
                        # 'overflow': 'visible',
                        # 'font-size':'14px',
                    },
                    style_cell={
                        'font-family': 'sans-serif',
                        'overflow': 'hidden',
                        'minWidth': '100px',
                        # 'font-size':'14px',
                        # 'textOverflow': 'ellipsis',
                    },
                )
            ])
        ], className="subpage")
    ], className="page")


def graphit(pfullurl):
    # offset = page * DEFAULT_ROWS_PER_PAGE
    q = parseurl(pfullurl)
    # Grab jobid values from query dict
    page = q['jobid'][:]
    # , limit=DEFAULT_ROWS_PER_PAGE, offset=page*DEFAULT_ROWS_PER_PAGE
    job_df = JobGen(jobs=page).jobs_df
    jobids = q.get('jobid', None)
    group_by = q.get('groupby', 'tag-op')[0]
    exe_query = q.get('exes', None)
    logger.debug("groupby: {}".format(group_by))
    logger.debug("exe_query: {}".format(exe_query))
    table_data = job_df
    if jobids:
        if job_df['job id'].isin(jobids).any():
            logger.debug("Job found, converting tags to print")
            table_data = job_df.loc[job_df['job id'].isin(jobids)]
            table_data['tags'] = table_data['tags'].apply(dumps)
    from functions import durList, separateDataBy
    newData, exenames, traceList = durList(jobids[0], 0, 1000000, exe_query)
    outputData = separateDataBy(newData, group_by)  # exename or option from traceList
    return html.Div([
        html.Div([
            # dcc.Location(id='url', refresh=False),
            html.Div([
                dcc.Graph(figure={
                    'data': outputData,
                    'layout': {
                        'title': 'Job {}'.format(jobids[0] if len(jobids) < 2 else ', '.join(jobids)),
                        'yaxis': {
                            'type': 'log'
                        }
                    }
                }, id='chart'),
                dash_table.DataTable(
                    id='custom-table',
                    columns=[
                        {"name": i, "id": i} for i in sorted(table_data.columns)
                    ],
                    sort_action='native',
                    data=table_data.to_dict('records'),
                    fixed_rows={'headers': True, 'data': 0},
                    # fixed_columns={ 'headers': True, 'data': 1 },#, Css is not setup for this
                    style_table={
                        'padding': '5px',
                        'height': '300px',
                        'font-size': '14px'
                    },
                    style_header={
                        'font-weight': 'bold',
                        'padding': '5px',
                        'whiteSpace': 'normal',
                        # 'overflow': 'visible',
                        # 'font-size':'14px',
                    },
                    style_cell={
                        'font-family': 'sans-serif',
                        'overflow': 'hidden',
                        'minWidth': '100px',
                        # 'font-size':'14px',
                        # 'textOverflow': 'ellipsis',
                    },
                ),
            ])
        ], className="subpage")
    ], className="page")


def graph_plotly(url):
    """
    Based on the url this function will return the page including
    the graph data.  Parsing the url and passing it onto the appropriate
    functions.
    Currently supports gantt & boxplot url options.

    gantt:
        graph_plotly('http://localhost:8050/graph/gantt/804278?tags=op_instance:16,op_instance:10')

    boxplot:
        graph_plotly('http://localhost:8050/graph/boxplot/model_sample?jobs=job1,job2&normalize=True')
    """
    jobname = None
    metric = None
    exp_component = None
    e = parse_url(url)
    logger.debug(e)
    path = e['path']
    query = e['query']
    graph_style = path[1]
    # Return a gantt chart
    if graph_style == 'gantt':
        default_tags = ['op_instance', 'op']
        jobname = query.get('expname', [None])[0]
        exp_name = query.get('expname', [None])[0]
        jobs = query.get('jobs', None)
        exp_component = query.get('exp_component', [None])[0]
        # if len(path)>2:
        # job = path[2]
        gtags = query.get('tags', None)
        graph_data = create_gantt_graph(
            jobs,
            gtags if gtags else default_tags,
            exp_name=exp_name,
            exp_component=exp_component)
    # Return a boxplot graph
    elif graph_style == 'boxplot':
        model = ""
        jobs = []
        # Assign url value after /boxplot/ to model name
        if len(path) > 2:
            model = path[2]
        tags = query.get('tags', ['op'])
        logger.debug("Tags requested: {}".format(tags))
        graph_data = create_boxplot(
            model=model, jobs=query.get('jobs'), normalize=query.get(
                'normalize', ['True'])[0], metric=query.get(
                'metric', ['cpu_time'])[0], tags=tags)
    # Return a bar graph
    elif graph_style == 'bar':

        # Rename and retrieve parameters
        y_value = 'component'
        jobname = query.get('expname', None)[0]
        bar_title = "exp_name:" + jobname
        metric = query.get('metric', ['duration'])
        sort = query.get('sort', [None])[0]
        if sort is None:
            logger.debug("Sort by first metric, sort not supplied")
            sort = metric[0] if isinstance(metric, list) else metric
        limit = int(query.get('limit', [0])[0])
        tag_dict = {'exp_name': jobname}
        exp_component = query.get('exp_component', [None])[0]
        ops = query.get('op', [None])[0]
        jobs = query.get('jobs', None)
        if ops:
            y_value = ops
            bar_title = "Operations in job:'" + \
                ','.join(jobs) + "' component: '" + exp_component + "' experiment: '" + jobname + "'"
            graph_plot = graph_ops(jobs=jobs, tag_value=ops, metric=metric, title=bar_title)
        elif exp_component or jobs:
            bar_title = "Jobs in component: '" + exp_component + "' in experiment: '" + jobname + "'"
            tag_dict.update({'exp_component': exp_component})
            logger.debug("Requested tag_dict {}".format(tag_dict))
            y_value = 'jobid'
            graph_plot = graph_components(
                exp_name=jobname,
                exp_component=exp_component,
                jobs=jobs,
                title=bar_title,
                metric=metric,
                order=sort)
        elif jobname:
            bar_title = "Components in experiment: '" + jobname + "'"
            graph_plot = graph_experiment(exp_name=jobname, title=bar_title, metric=metric)

        # Convert plot into graph object
        # and check if plot was generated
        if graph_plot:
            graph_data = dcc.Graph(figure=graph_plot, id='bargraph')
        else:
            graph_data = "Something went wrong..."
        # grouped = True if len(metric) > 1 else False #query.get('grouped',[False])[0]
        # Build and store a graph of given parameters
        # graph_data = create_grouped_bargraph(title=bar_title, jobs=jobs, tags=tag_dict, metric=metric, ops=ops, order_by=order_by,limit=limit, y_value=y_value)

    else:
        graph_data = 'Unknown graphstyle'

    return html.Div(
        [
            # represents the URL bar, doesn't render anything
            # refresh causes page to reload if path is updated via callback
            dcc.Location(id=graph_style + '-url', refresh=True),
            html.Div(style={'inline': 'true'}, children=[
                Header(),
            ]),
            html.Div(id="subpage", children=[
                html.Button(id='Send-hidden-to-nb', children=["Send To Notebook"], n_clicks_timestamp=0,
                            style={'backgroundColor': '#20c997', 'color': '#020080'}),
                html.Div(id='nb-link-div', children=''),
                # Return graph-area-stop if we are on final jobs page
                # To stop any new callbacks from firing
                html.Div(id="graph-area" if jobs else "graph-area", children=graph_data),
                html.Div(id="hidden-divs", children=[
                    # Expname - hidden div
                     html.Div(children=jobname, id='bar-expname', style={'display': 'none'}),
                     # metric - hidden div
                     html.Div(children=metric, id='bar-metrics', style={'display': 'none'}),
                     html.Div(children=exp_component, id='exp-component', style={'display': 'none'}),
                     html.Div(children=jobs, id='exp-jobs', style={'display': 'none'}),
                     # specify experiment as the level of bar graph
                     # if exp_component is empty
                     html.Div(children="experiment", id='bar-level', style={'display': 'none'})
                     # We have component but no jobs
                     if exp_component is None and jobs is None else
                     html.Div(children="component", id='bar-level', style={'display': 'none'})
                     if exp_component and jobs is None else
                     # Finally we're at job level of ops
                     html.Div(children="job", id='bar-level', style={'display': 'none'}),
                     ])
            ], className="subpage"),
            html.Pre(id='click-data', children=''),
            Footer(),
        ], className="page")


def compare(url):

    return html.Div([
        html.Div(children=url                 # Hidden Div full url
                 , id='fullurl', style={'display': 'none'}),
        dcc.Location(id='compare-url', refresh=False),
        html.Div(id='compare-zoom-jobs', style={'display': 'none'}),
        html.Div(id='jobs-in-view', style={'display': 'none'}),
        html.Div([
            dcc.Graph(id='scatter-compare',
                      figure={
                          'layout': {
                              'clickmode': 'select'
                          }
                      })]),
        html.Div([
            "X:",
            dcc.Dropdown(
                id='x-scatter-dropdown',
                options=[
                    {'label': 'duration', 'value': 'duration'},
                    {'label': 'cpu_time', 'value': 'cpu_time'},
                    {'label': 'start', 'value': 'start'}
                ],
                value='start')
        ], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([
            "Y:",
            dcc.Dropdown(
                id='y-scatter-dropdown',
                options=[
                    {'label': 'cpu_time', 'value': 'cpu_time'},
                    {'label': 'duration', 'value': 'duration'}
                ],
                value='cpu_time')
        ], className="subpage",
            style={'width': '49%', 'display': 'inline-block'}),
        html.Br(),
        html.Div([
            "Graph Style:",
            dcc.Dropdown(
                id='compare-type-dropdown',
                options=[
                    {'label': 'scatter', 'value': 'scatter'},
                    {'label': 'gantt', 'value': 'gantt'},
                    {'label': 'bar', 'value': 'bar'}
                ],
                value='scatter')
        ], style={'width': '20%', 'display': 'inline-block'})
    ], className="page")


def multi_flow(url):
    return html.Div([
        # hidden url for callback
        html.Div(children=url, id='fullurl', style={'display': 'none'}),
        html.Div([

            dcc.Graph(id='multi-flow-chart',
                      figure={
                          'data': [
                              {
                                  'x': [1, 2, 3, 4],
                                  'y': [4, 1, 3, 5],
                                  'text': ['a', 'b', 'c', 'd'],
                                  'customdata': ['c.a', 'c.b', 'c.c', 'c.d'],
                                  'name': 'Trace 1',
                                  'mode': 'markers',
                                  'marker': {'size': 12}
                              },
                              {
                                  'x': [1, 2, 3, 4],
                                  'y': [9, 4, 1, 4],
                                  'text': ['w', 'x', 'y', 'z'],
                                  'customdata': ['c.w', 'c.x', 'c.y', 'c.z'],
                                  'name': 'Trace 2',
                                  'mode': 'markers',
                                  'marker': {'size': 12}
                              }
                          ],
                          'layout': {
                              'clickmode': 'event+select'
                          }
                      },
                      style={'width': '100%'}),
        ]),
        html.Div([
            html.Div(
                id='job-flow-text',
                style={
                    'position': 'relative',
                    'left': '17%',
                    'width': '200px',
                    'display': 'inline-block',
                    'text-align': 'center'}),
            html.Div(
                ["operation"],
                id='op-flow-text',
                style={
                    'position': 'relative',
                    'left': '20%',
                    'width': '200px',
                    'display': 'inline-block',
                    'text-align': 'center'}),
            html.Div(
                ["process"],
                id='proc-flow-text',
                style={
                    'position': 'relative',
                    'left': '23%',
                    'width': '200px',
                    'display': 'inline-block',
                    'text-align': 'center'}),
            html.Div(
                ["thread"],
                id='thread-flow-text',
                style={
                    'position': 'relative',
                    'left': '27%',
                    'width': '200px',
                    'display': 'inline-block',
                    'text-align': 'center'}),
            html.Div(
                dcc.Slider(
                    id='zoom-level-multi-flow',
                    step=None,
                    min=0,
                    max=0,
                    value=0,
                    marks={
                        0: {'label': 'Job'},
                        1: {'label': 'Operation'},
                        2: {'label': 'Process'},
                        3: {'label': 'Thread'},
                    },
                    included=False,
                    # vertical=True,
                ), style={"width": '240px',
                          'height': '40px',
                          # 'margin': 'auto',
                          'position': 'relative',
                          'left': '25%'}),
            html.Div([
                "Metric:",
                dcc.Dropdown(
                    id='y-metric-multi-flow',
                    options=[
                        {'label': 'duration', 'value': 'duration'},
                        {'label': 'cpu_time', 'value': 'cpu_time'},
                        {'label': 'write_bytes', 'value': 'write_bytes'}
                    ],
                    value='duration'
                )], style={'inline': 'true'})
        ], style={'inline': 'true'})  # div
    ])  # outer div


######################## END table Layout ########################


######################## 404 Page ########################
noPage = html.Div([
    # CC Header
    # Header(),
    html.P(["404 Page not found"])
], className="no-page")
