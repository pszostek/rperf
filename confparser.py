import re
def get_indent(line):
    spaces = 0
    tabs = 0
    for char in line:
        if char == ' ':
            spaces += 1
        elif char == '\t':
            tabs += 1
    return tabs + spaces/4

def line_split(line):
    line = line.strip()
    ret = []
    if line.startswith('['):
        return ['[']+line_split(line[1:])
    elif line.endswith(']'):
        return line_split(line[:-1])+[']']
    #
    # remove literals
    #
    m = re.match('([^"]*)"(.*?)"', line)
    if m:
        return line_split(m.group(1)) + [m.group(2)]
    parts = [part.strip() for part in line.split(':')]
    return list((parts[0], ':', line_split(parts[1])))
    

def parse(text):
    lines = [line for line in text.split('\n') if line and line[0] != '#']
    stack = []
    for line in lines:
        indent = get_indent(line)
        parts = line_split(line)
        print parts

text = """
events: L1-dcache-load-misses, r01D3, r04D3
pfm-events: LLC_REFERENCES, LLC_MISSES
host: localhost
env: GOMP_CPU_AFFINITY=0-55
precmd: source /root/benchmarking/setup.sh
"""

parse(text)

