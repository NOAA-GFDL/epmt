"""functions.py
Methods used for data manipulation from graphing to charting
"""

from urllib.parse import parse_qs, urlparse
from math import log
import pandas as pd
import time
from ..dash_config import MOCK_EPMT_API
from logging import getLogger
logger = getLogger(__name__)
# pd.options.mode.chained_assignment = None


class InterfaceError(Exception):
    pass


if MOCK_EPMT_API:
    logger.info("Using Mock API")
    from ..epmt_query_mock import get_procs, get_ops, get_refmodels, get_jobs
else:
    logger.info("Using EPMT API")
    from epmt.epmt_query import get_procs, get_ops, get_refmodels, get_jobs

# Return dictionary query results


def parseurl(i):
    """
    This Function uses urllib to parse a query then
    checks each of the query keys values for commas and converts
    those values into lists.
    Accepts url & returns query dictionary.
    """
    logger.info("Given URL {}".format(i))

    # Determine graphstyle and jobid

    # convert url into dictionary
    res_dict = parse_qs(urlparse(i).query)
    logger.debug("URL parse_qs: {}".format(res_dict))
    # Parse query key values, values for commas
    # TODO: A better method may be encoding or repeating the key
    # https://stackoverflow.com/a/50537278
    for field in res_dict.keys():
        if ',' in res_dict[field][0]:
            res_dict[field] = res_dict[field][0].split(',')
    logger.info("URL2Dict {}".format(res_dict))
    return res_dict


def recent_button(btn_dict):
    """
    This function accepts a dictionary of buttons, tabs and timestamps
     and returns the most recent one clicked.
    Input: dictionary of buttons timestamps
    If Model tab was clicked:
     {'button1':0, 'button2':0, 'button3':0, 'tabs':'model'}
    If Button2 was clicked recently:
      {'button1':352512, 'button2':952512, 'button3':152512, 'tabs':None}
    Returns: Recent button clicked or None
    """
    # If Tab was clicked return none
    if btn_dict.get('tabs'):
        logger.debug("Tab {} Was clicked".format(btn_dict['tabs']))
        return None
    else:
        # Tab was not clicked remove it from dictionary for latter max calculation
        btn_dict.pop('tabs', None)
    if sum(btn_dict.values()) > 0:
        recent = max(btn_dict, key=lambda key: btn_dict[key])
        logger.debug("Button click {}".format(recent))
        return recent
    return None


power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T', 5: 'P'}


def get_unit(alist):
    """

    Get greatest unit from df alist

    """
    if len(alist) > 0:
        hi = max(alist)
    else:
        hi = 1
    # print(alist)
    return power_labels[int(log(hi, 1024))]


def convtounit(val, reqUnit):
    """

    This function converts a given byte to requested unit.
    Helper function accepts a value & a unit
    Input: bytes, power_label unit
    Output: Value converted without label.

    """
    # Letter to Unit reverse search
    unitp = list(power_labels.keys())[list(power_labels.values()).index(reqUnit)]
    return val / 1000**unitp


def durList(jid, minDur, maxDur, exes):
    """

    This Function will convert jobid's into traces for graphing.
    Takes jobid, and limiting paramaters for query

    minDur & maxDur will reduce the procs returned to only
    those requested.

    exes will be a list of those exename procs that are requested

    """
    logger.debug("Building data Dict for {}".format(jid))
    logger.debug("Querying DB...")
    start = time.time()
    # TODO
    proc_limit = None
    logger.warning("Limiting procs to {}".format(proc_limit))
    # , fltr=lambda p: p.duration > minDur and maxDur > p.duration, order='desc(p.exclusive_cpu_time)', fmt='dict')
    procList = get_procs(jid, limit=proc_limit)
    end = time.time()
    print("Took", (end - start))
    # print("Sorting and Filtering ",len(procList))
    # procList = procList[0::density]
    # print("After ", len(procList))
    # print("loop:",tuple(i for i in options))
    # x value is start time, y variable index on options
    exenames = list(set([k['exename'] for k in procList]))
    opnames = set(k['tags'].get('op', 'no-tag') for k in procList)
    traceList = [{'label': 'Executable Name', 'value': 'exename'},
                 {'label': 'Job', 'value': 'job'},
                 {'label': 'Host', 'value': 'host'},
                 {'label': 'Exit Code', 'value': 'exitcode'}]
    for n in opnames:
        traceList.append({'label': n.capitalize(), 'value': "tag-" + n})
    exenames.sort(key=str.lower)
    if (exes):  # Leaves alot of empty dicts, dropdown seems to ignore them
        filteredData = []
        for x in procList:
            if x['exename'] in exes:
                filteredData.append(x)
        # old xyData = [{key:val for key, val in e.items() if val[1] in exes} for e in procList]
        procList = filteredData
    return procList, exenames, traceList


def separateDataBy(data, graphStyle="exename", pointText=("path", "exe", "args"), ):
    """
    This function takes traceLists as data and transposes them
    by the requested graphstyle.
    Graphstyle options are tag-ops, exename or other tag paramaters
    (tag-instance or tag-sequence)

    """
    from collections import defaultdict
    outputDict = defaultdict(list)
    # print("graphstyle", graphStyle)
    for entry in data:
        if (graphStyle[:4] == "tag-"):
            # Works but dirty
            # outputDict[sum(entry[graphStyle].items(),())].append([entry])
            if entry['tags'].get('op', None):
                outputDict[entry['tags'][graphStyle[4:]]].append([entry])
        else:
            outputDict[entry[graphStyle]].append([entry])
        # print([sublist[0]['start'] for sublist in outputDict['dash']])
    hoverwidth = 20
    output = [{  # 'x': list(range(1, 11)), 'y': list(range(1, 11)),
              'mode': 'markers',
              'x': [sublist[0]['start'] for sublist in outputDict[n]],
              'y': [sublist[0]['duration'] for sublist in outputDict[n]],
              'text' if (True) else None: [[sublist[0]["exename"], sublist[0]["args"], sublist[0]["path"]] for sublist in outputDict[n]],
              # 'hoverinfo':"text",
              'hovermode': False,
              'name': n,
              # 'hovertemplate': "Path: %{text[2]}<br>" +
              #                  "Args: <br>%{text[1]}"
              # 'textposition': 'top center'
              } for n in outputDict.keys()]
    # print(output)
    #
    # textwrap.wrap("Path: %{text[2]}<br>" + "Args: <br>%{text[1]}", hoverwidth)
    # print("args: {0}".format("<br>".join(textwrap.wrap(longstring,hoverwidth))))
    return output


def df_normalizer(df, idx='op', norm_metric='cpu_time'):
    """
    Simple mean normalizer algorithm that works on dataframes.
    Will create a new column titled the value from norm_metric
    Normalized Column: (norm_metric)_normalized
    """
    df = df.set_index(idx)
    means_stds = df.groupby(idx)[norm_metric].agg(['mean', 'std']).reset_index()
    df = df.merge(means_stds, on=idx)
    df[norm_metric + '_normalized'] = (df[norm_metric] - df['mean']) / df['std']
    return df

# Incomplete, could be extended to apply text to each bar on gantt chart
# x_pos/y_pos need work
# def addAnnot(df, fig):
#     for i in df:
#         x_pos = (i['Finish'] - i['Start'])/2 + i['Start']
#         for j in fig['data']:
#             y_pos = (j['y'][0] + j['y'][1] + j['y'][2] + j['y'][3])/4
#         fig['layout']['annotations'] += tuple([dict(x=x_pos,y=y_pos,text=i['Task'],font={'color':'black'})])
#     return fig


def generate_notebook(values, depth='jobs'):
    from nbformat.v4.nbbase import (
        new_code_cell, new_markdown_cell, new_notebook,
        new_output, new_raw_cell
    )
    # This is a relative link to the notebooks location and initial
    # File name, subsequent files will be made with name-n.ipynb where n
    # is an integer begining with 1
    from tempfile import NamedTemporaryFile
    nbpath = NamedTemporaryFile(suffix='.ipynb', prefix='dash_nb-', dir='./notebooks/')  # , delete=False)
    cells = []
    cells.append(new_markdown_cell(
        source='Import EPMT: ',
    ))
    count = 1
    cells.append(new_code_cell(
        source="""# epmt_query contains the EPMT Query API
from epmt import epmt_query as eq
# epmt_outliers contains the EPMT Outlier Detection API
from epmt import epmt_outliers as eod
# epmt_stat contains statistical functions
from epmt import epmt_stat as es""",
        execution_count=count,
    ))
    count = count + 1
    if 'tags' in values:
        cells.append(new_code_cell(
            source="tags = " + str(values['tags']),
            execution_count=count,
        ))
        count = count + 1
    if 'jobs' in values:
        cells.append(new_code_cell(
            source="jobs = " + str(values['jobs']),
            execution_count=count,
        ))
        count = count + 1
    if depth == 'jobs':
        cells.append(new_code_cell(
            source="""j = eq.get_jobs(tags=tags{}, fmt='pandas')
j""".format(", jobs=jobs" if 'jobs' in values else ''),
            execution_count=count,
        ))
        count = count + 1
    if depth == 'ops':
        cells.append(new_code_cell(
            source="op_tags='op'",
            execution_count=count,
        ))
        count = count + 1
        cells.append(new_code_cell(
            source="eq.get_ops(jobs=jobs, tags=op_tags, fmt='pandas')",
            execution_count=count,
        ))
        count = count + 1
    nb0 = new_notebook(cells=cells,
                       metadata={
                           'language': 'python',
                       }
                       )
    import nbformat as nbf
    import codecs
    f = codecs.open(nbpath.name, encoding='utf-8', mode='w')
    nbf.write(nb0, f, 4)

    from os.path import relpath

    return relpath(nbpath.name, '.')
