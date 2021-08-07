from sys import stdout


def progress(current: int, total: int, prefix: str):
    p = current * 100 // total
    stdout.write('\r{0}: [{1}{2}] {3}% - {4}/{5}'.format(prefix, '#'*(p//2), '-'*(50-p//2), p, current, total))
