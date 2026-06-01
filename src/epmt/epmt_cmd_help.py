"""
EPMT help command module - provides help functionality.
"""
from inspect import signature
from sys import stderr

def epmt_help_api(funcs=None):
    """
    Provide help documentation for EPMT API functions.
    """
    if funcs is None:
        funcs = []
    import epmt.epmt_query as eq
    import epmt.epmt_outliers as eod
    import epmt.epmtlib as el
    import epmt.epmt_stat as es
    import epmt.epmt_exp_explore as exp
    from epmt.epmtlib import docs_module_index

    if funcs:
        for fname in funcs:
            func = None
            for m in (eq, eod, es, el, exp):
                if hasattr(m, fname):
                    func = getattr(m, fname)
                    break
            if func:
                print(f"from {m.__name__} import {fname}\n")
                section = el.docs_func_section(func)
                print(f"{func.__name__}{signature(func)}")
                doc = func.__doc__
                if section:
                   # add the section name with suitable indent
                    print(f'\n    Section::{section}')
                    # remove the ugly section suffix from the summary string
                    doc = doc.replace(f'::{section}', '')
                print(doc, '\n\n')
            else:
                print(f'Could not find function {fname} in any module', file=stderr)
    else:
        for m in (eq, eod, exp, es):
            print(m.__doc__)
            print(docs_module_index(m, fmt='string'), '\n')
