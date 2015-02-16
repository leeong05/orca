from orca.combiner.regression import (
        OLSCombiner,
        QuantRegCombiner,
        RidgeCombiner,
        LassoCombiner,
        )

groups = {
        'wdfund': {
            'combiner': OLSCombiner(20),
            'alphas': {
                'wdfund1': {'path': 'wdfund1.msgpack'},
                'wdfund2': {'path': 'wdfund2.msgpack'},
                'wdfund3': {'path': 'wdfund3.msgpack'},
                'wdfund4': {'path': 'wdfund4.msgpack'},
                'jyfund1': {'path': 'jyfund1.msgpack'},
                'jyfund2': {'path': 'jyfund2.msgpack'},
                'jyfund3': {'path': 'jyfund3.msgpack'},
                'jyfund4': {'path': 'jyfund4.msgpack'},
                'jyfund5': {'path': 'jyfund5.msgpack'},
                },
            },
        }

group_combiner = {
        'combiner': OLSCombiner(20),
        'groups': None,
        'output': 'wdfund.msgpack',
        'filetype': 'msgpack',
        }
