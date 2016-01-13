"""Dummy helper functions"""
# author: Pawel Szostek

from copy import deepcopy
try:
    from collections import OrderedDict
except ImportError:
    from OrderedDict import OrderedDict


def integerize(number):
    if isinstance(number, int):
        return number
    else:
        if number-int(number)<0.00001: # the number is almost an integer
            return int(number)
        else:
            return number

def cut_out_comments(hostfile_lines):
    """Cuts out comments from json config files"""
    output = []
    for line in hostfile_lines:
        line = line.strip()
        if '#' in line:
            parts = line.split('#')
            not_commented_out = parts[0]
            if not_commented_out:
                output.append("%s\n" % not_commented_out)
        else:
            output.append(line)
    return output

def flip_dictionary(ddict):
    """Flips keys versus values in a dictionary"""
    # ddict is a dictionary of dictionaries
    # of the shape: {a:{b:1,c:1,d:1}, e:{b:2,c:2,d:2}}
    # the function turns it into {b:{a:1,e:2},c:{a:1,e:2},d:{a:1,e:1}}
    keys1 = ddict.keys()
    keys2 = ddict[keys1[0]].keys()
    rdict = OrderedDict()
    for key in keys2:
        rdict[key] = OrderedDict()
    for key1, value1 in ddict.iteritems():
        for key2, value2 in value1.iteritems():
            rdict[key2][key1] = value2
    return rdict

def median(lst):
    """Median to avoid using numpy"""
    lst = sorted(lst)
    if len(lst) < 1:
        return None
#    if len(lst) %2 == 1:
    else:
        return lst[((len(lst)+1)/2)-1]
    # else:
    #     return float(sum(lst[(len(lst)/2)-1:(len(lst)/2)+1]))/2.0

def mean(lst):
    """Arithmetic mean"""
    return sum(lst) / float(len(lst))

def _ss(data):
    """Return sum of square deviations of sequence data."""
    mean_value = mean(data)
    sum_of_sq_devs = sum((x-mean_value)**2 for x in data)
    return sum_of_sq_devs

def stddev(data):
    """Calculates the population standard deviation."""
    length = len(data)
    if length < 2:
        raise ValueError('variance requires at least two data points')
    sum_of_sq_devs = _ss(data)
    pvar = sum_of_sq_devs/length # the population variance
    return pvar**0.5

