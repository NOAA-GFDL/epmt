#!/usr/bin/env python3

# Taken and extended from well researched SO post
# https://stackoverflow.com/a/25496309/10377587

import urllib.parse as urlp
import posixpath
from logging import getLogger
from ..dash_config import MOCK_EPMT_API
logger = getLogger(__name__)

if MOCK_EPMT_API:
    from ..epmt_mock import tag_from_string
else:
    from epmt.epmtlib import tag_from_string


def path_parse(path_string, *, normalize=True, module=posixpath):
    # TODO: This while loop causes infinite loops under malformed urls
    # Note the whitespace before http:
    #  ex: "    http://192.168.1.147:8050/graph/gantt/804278?tags=op_instance:16,op_instance:20,op:hsmget,op_sequence:10"
    # path_string becomes //192.168.1.147:8050/graph/gantt/804278
    #

    result = []
    if path_string:
        if normalize:
            tmp = module.normpath(path_string)
        else:
            tmp = path_string
        while tmp != "/":
            (tmp, item) = module.split(tmp)
            result.insert(0, item)
    return result


def parse_url(url, *, normalize=True, module=posixpath):
    """
    Accepts string url and returns a dictionary with path and query.
    Where path is a list of strings of each subdirectory and query is a
    dictionary of the requested parameters.

    parse_url('http://172.17.0.1:8050/graph/boxplot/test_model/?jobs=685016&normalize=False&metric=duration')

    returns:
    {'path': ['graph', 'boxplot', 'test_model'],
    'query': {'jobs': ['685016'], 'normalize': ['False'], 'metric': ['duration']}}

    Lists of jobids or query values:
    The query parameters values will be broken into list if commas are
    included in the query.

    Boxplot Example with list of jobs:
    A boxpot with multiple jobs to plot against a model
    http://localhost:8050/graph/boxplot/model_sample?jobs=job1,job2&normalize=True
    returns:
    {'path': ['graph', 'boxplot', 'model_sample'],
    'query': {'jobs': ['job1', 'job2'], 'normalize': ['True']}}

    Gantt Example with list of operations:
    A timeline graph with multiple specific ops
    http://192.168.1.147:8050/graph/gantt/804278?tags=op_instance:16,op_instance:20,op:hsmget,op_sequence:10
    returns:
    {'path': ['graph', 'gantt', '804278'],
    'query': {'tags': ['op_instance:16',
    'op_instance:20',
    'op:hsmget',
    'op_sequence:10']}}
    """
    # Clean url of whitespace before and after
    url = url.strip()
    # url must start with 'http://' else host becomes part of path
    # according to urlp.urlparse
    if not url.startswith('http'):
        url = "http://" + url

    url_parsed = urlp.urlparse(url)

    path_parsed = path_parse(urlp.unquote(url_parsed.path),
                             normalize=normalize, module=module)

    query_parsed = urlp.parse_qs(urlp.urlparse(url).query)

    # Parse query key values, values for commas
    # TODO: A better method may be encoding or repeating the key
    # https://stackoverflow.com/a/50537278
    for field in query_parsed.keys():
        logger.debug("Parsing field {} for {}".format(field, query_parsed[field]))
        if ',' in query_parsed[field][0]:
            query_parsed[field] = query_parsed[field][0].split(',')
        elif ':' in query_parsed[field]:
            logger.debug("Query dict found: {}".format(query_parsed[field]))
            query_parsed[field] = tag_from_string(query_parsed[field])
            logger.debug("Query dict converted: {}".format(query_parsed[field]))
    return {"path": path_parsed, "query": query_parsed}


def url_gen(graph_type='', jobs=[], model='', parameters=[], host='localhost', port=8050):
    """
    Generate a url for graphing jobs and models

    graph_type: This is a string of the requested graph type.

                Example of a boxplot:
                    url_gen('boxplot', ['job1','job2'], 'model_test')

    jobs:
        list of jobs ['joba','jobb']
    model:
        string model name 'Sample model 123'
    paramaters:
        Depend on which graph type you're using.
            gantt:
                ['tags=op,op_instance']
            boxplot:
                ['normalize=True']
    host:
        hostname or ip of dash hosting server
    port:
        port dash server is running on, can be int and will be
        converted to str
    """
    urlprefix = '/graph/'
    urlsuffix = ''

    if graph_type not in ['gantt', 'boxplot', 'radar']:
        return "Bad graph type or incomplete request"

    # gantt suffix will be a single jobid
    if graph_type == 'gantt':
        urlsuffix = '/' + ','.join(jobs)
        if model:
            # Logger info
            print("Model will be ignored")
        query = '&'.join(parameters)

    # boxplot
    # suffix will be a single model
    # Apply jobs as comma separated query
    if graph_type == 'boxplot':
        urlsuffix = '/' + model

        # Convert jobs into comma separated
        if jobs:
            query = 'jobs=' + ','.join(jobs)

        # Convert list of parameters into ampersand spaced
        # Surely a better way to do this
        if parameters:
            query = query + '&' + '&'.join(parameters)

    netloc = host + ':' + str(port)
    path = urlprefix + graph_type + urlsuffix

    url_parts = ['http', netloc, path, '', query, '']
    print(url_parts)
    result = urlp.urlunparse(url_parts)
    return result


# parse_url("http://eg.com/hithere/something/else")
# parse_url("http://eg.com/hithere/something/else/")
# parse_url("http://eg.com/hithere/something/else/", normalize=False)
# parse_url("http://eg.com/see%5C/if%5C/this%5C/works", normalize=False)
# parse_url("http://eg.com/see%5C/if%5C/this%5C/works", normalize=False, module=ntpath)
