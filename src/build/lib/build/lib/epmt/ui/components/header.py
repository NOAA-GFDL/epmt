# import dash_html_components as html
from ..jobs import get_version
from dash import html
# import dash_core_components as dcc
from dash import dcc
import dash_bootstrap_components as dbc
import dash_daq as daq


def Header():
    return html.Div([
        # get_logo(),
        get_header(),
        html.Br([]),
        html.Div(),
        # get_menu()
    ])


def Footer():
    # https://gitlab.com/minimal-metrics-llc/epmt/epmt-dash/issues
    return html.Div(
        [
            "Experiment Performance Management Tool - ",
            html.Div(
                id='version',
                children=[
                    dcc.Link(
                        get_version() +
                        " Issue Tracker",
                        href='https://gitlab.com/minimal-metrics-llc/epmt/epmt-dash/issues')],
                style={
                    'width': '49%',
                    'display': 'inline-block'})])


def get_logo():
    logo = html.Div([

        html.Div([
            html.Img(
                src='https://pbs.twimg.com/profile_images/779313079289077761/f2YOWoLW_400x400.jpg',
                height='101',
                width='141')
        ], className="ten columns padded"),

        # html.Div([
        #     dcc.Link('Full View   ', href='/cc-travel-report/full-view')
        # ], className="two columns page-view no-print")

    ], className="row gs-header")
    return logo


def get_header():
    header = html.Div([

        html.Div([
            html.H5(
                'Experiment Performance Management Tool')
        ], className="twelve columns padded")

    ], className="row gs-header gs-text-header")
    return header


def get_menu():
    navbar = dbc.Nav(
        children=[
            dbc.NavItem(dbc.NavLink("Recent Jobs", href="/")),
            dbc.NavItem(
                dbc.NavLink(
                    "Test Boxplot",
                    href="/graph/boxplot/atmos_model_hist/?jobs=2460339&normalize=False")),
            dbc.NavItem(dbc.NavLink("Test Gantt", href="/graph/gantt/2494106")),
            dbc.NavItem(dbc.NavLink("Test Scatter", href="/graph/boxplot/?jobs=2494106&normalize=False")),
            dbc.NavItem(
                dbc.NavLink(
                    "Test Bar graph",
                    href="/graph/bar?expname=ESM4_hist-piAer_D1&metric=duration,cpu_time"))
        ],
        fill=True,
        pills=True)
    return navbar

    # dcc.Linhtml.Div(k('Overview - Unprocessed Jobs', href='/unprocessed/', className="tab"),
