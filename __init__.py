#!/usr/bin/python

__all__ = ['core', 'commands', 'hash']

if __name__ == '__main__':
    import os, sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from pysync import commands
    commands.main(sys.argv)
