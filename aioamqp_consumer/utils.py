def unpartial(fn):
    while hasattr(fn, 'func'):
        fn = fn.func

    return fn
