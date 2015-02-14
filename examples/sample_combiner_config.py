groups = {
        'g1': {
            'combiner': 'Combiner(5)',
            'alphas': {
                'alpha1': {
                    'path': 'file_path_to_alpha1',
                    'filetype': 'msgpack',
                    },
                },
            },
        }

group_combiner = {
        'combiner': 'Combiner(10)',
        'groups': None,
        'output': 'output',
        'filetype': 'msgpack',
        }
