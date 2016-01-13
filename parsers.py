"""Parsers for perf, time etc."""

from helper import integerize

try:
    from collections import OrderedDict
except ImportError:
    from OrderedDict import OrderedDict

def parse_notool(stdout, stderr, numeric_output=False):
    results = {}
    if numeric_output:
        results['output'] = integerize(float(stdout))
    return results

def parse_perf(stdout, stderr, numeric_output=False):
    """Returns a series of values for perf statistics"""
    lines = [line.strip() for line in stderr.split("\n")]
    results_started = False
    results = OrderedDict()
    for line in lines:
        if results_started is False:
            if line.startswith("Performance counter"):
                results_started = True
                continue
            else:
                continue

        parts = line.split()
        if "seconds time elapsed" in lines:
            break
        elif not line:
                continue
        elif line.startswith("<not supported>"):
            results[parts[2]] = None  # <not supported> name
        elif line.endswith("seconds time elapsed"):
            results["time"] = float(parts[0])
        else:
            try:
                value = int(parts[0].replace("'", '').replace(',', ''))
                # get rid of commas
                results[parts[1]] = value
            except ValueError: # it's a float, not an int
                value = float(parts[0].replace("'", '').replace(',', ''))
                results[parts[1]] = value
            except IndexError:  # OK, there is nothing in this line
                pass
    if numeric_output:
        results['output'] = integerize(float(stdout))
    return results

def parse_time(stdout, stderr, numeric_output=False):
    def splitmany(s, delims):
        """Splits a string by all the delimeters in delim"""
        last_split=0
        parts = []
        for i,l in enumerate(s):
            if l in delims:
                parts.append(s[last_split:i+1])
                last_split = i+1
        return parts

    def strtotime(s):
        """Translates time string to a float expressing seconds"""
        time = 0.
        parts = s.split('.')
        time += float(parts[-1])/100. 
        remainder = parts[0]
        if ':' in remainder: # 0:01.28
            parts = parts[0].split(':')
            parts = [item for item in reversed(parts)]
            time += float(parts[0])  # seconds

            try:
                time +=       60*float(parts[1]) # minutes 
                time +=    60*60*float(parts[2]) # hours
                time += 60*60*24*float(parts[3]) # days
            except IndexError:
                pass  # there is no days or hours
        else: # 0.00
            time += float(parts[0])

        return time

    lines = stderr.split('\n')
    line = lines[-2]

    """
    0.00 0.00 0:01.28
    """
    parts = line.split()

    user = strtotime(parts[0])
    system = strtotime(parts[1])
    elapsed = strtotime(parts[2])

    results = {"user": user,
            "system":system,
            "wall" :elapsed}

    if numeric_output:
        results['output'] = integerize(float(stdout))

    return results


