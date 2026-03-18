# Collection of EPMT commands for mock execution

from logging import getLogger, basicConfig, DEBUG, ERROR, INFO, WARNING
logger = getLogger(__name__)  # you can use other name


# Taken from epmtlib.py
# we assume tag is of the format:
#  "key1:value1 ; key2:value2"
# where the whitespace is optional and discarded. The output would be:
# { "key1": value1, "key2": value2 }
#
# We can also handle the case where a value is not set for
# a key, by assigning a default value for the key
# For example, for the input:
# "multitheaded;app=fft" and a tag_default_value="1"
# the output would be:
# { "multithreaded": "1", "app": "fft" }
#
# Note, both key and values will be strings and no attempt will be made to
# guess the type for integer/floats
def tag_from_string(s, delim=';', sep=':', tag_default_value='1'):
    from pony.orm.ormtypes import TrackedDict
    if type(s) in (dict, TrackedDict):
        return s
    if not s:
        return (None if s is None else {})

    logger = getLogger(__name__)
    tag = {}
    for t in s.split(delim):
        t = t.strip()
        if sep in t:
            try:
                (k, v) = t.split(sep)
                k = k.strip()
                v = v.strip()
                tag[k] = v
            except Exception as e:
                logger.warning('ignoring key/value pair as it has an invalid format: {0}'.format(t))
                logger.warning("%s", e)
                continue
        else:
            # tag is not of the format k:v
            # it's probably a simple label, so use the default value for it
            tag[t] = tag_default_value
    return tag
