def parallel_map(fun, iterable):
    return map(fun, iterable)


def parallel_run(fun, iterable):
    return [*map(fun, iterable)]
