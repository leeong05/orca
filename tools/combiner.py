"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.utils.io import (
        read_frame,
        dump_frame,
        )

def fit_by_group(groups, start, end):
    for group, combiner in groups.iteritems():
        combiner.prepare_data()
        combiner.normalize()
        X, Y = combiner.get_XY(start, end)
        groups[group] = combiner.fit(X, Y)
    return groups

def combine_groups(group_combiner, groups, start, end):
    if group_combiner is None:
        return groups.values()[0]

    for group, X in groups.iteritems():
        group_combiner.add_alpha(group, X)
    group_combiner.prepare_data()
    group_combiner.normalize()
    X, Y = group_combiner.get_XY(start, end)
    return group_combiner.fit(X, Y)


if __name__ == '__main__':
    import argparse
    import imp

    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='A Python configuration script file')
    parser.add_argument('-s', '--start', help='IS startdate', type=str)
    parser.add_argument('-e', '--end', help='IS enddate', type=str)
    parser.add_argument('-o', '--output', help='Output file name', type=str)
    parser.add_argument('-t', '--filetype', choices=('csv', 'msgpack', 'pickle'), default='msgpack')
    args = parser.parse_args()


    mod = imp.load_source('config',  args.file)
    groups = {}
    for group, config in mod.groups.iteritems():
        combiner = config['combiner']
        for name, alpha_config in config['alphas'].iteritems():
            alpha = read_frame(alpha_config['path'], alpha_config.get('filetype', None))
            combiner.add_alpha(name, alpha)
        groups[group] = combiner

    if 'group_combiner' not in mod.__dict__:
        try:
            assert len(groups) == 1
        except:
            print 'For multiple groups, you should specify "group_combiner" in', args.file
            raise
        group_combiner = None
        output = args.output
        filetype = args.filetype
    else:
        if len(groups) > 1:
            group_combiner = mod.group_combiner['combiner']
            groups_ = mod.group_combiner['groups']
            if groups_:
                groups = {k: v for k, v in groups.iteritems() if k in groups_}
        if len(groups) == 1:
            print 'With only one group specified, the "group_combiner" is not used'
            group_combiner = None
        output = mod.group_combiner.get('output', args.output)
        filetype = mod.group_combiner.get('filetype', args.filetype)

    groups = fit_by_group(groups, args.start, args.end)
    X = combine_groups(group_combiner, groups, args.start, args.end)
    dump_frame(X, output, filetype)
    print 'Saved in file', output
