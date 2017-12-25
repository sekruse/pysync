import time, sys, os, datetime, argparse, fnmatch, core

def index(args):
    starttime = time.time()
    excludes = [fnmatch.translate('.pysync')]
    if args.excludes: excludes += map(fnmatch.translate, args.excludes)
    index1 = core.create_index(args.directory, excludes=excludes, archive_path=os.path.join(args.directory, '.pysync'))
    elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
    print 'Loaded index with {} files in {}.'.format(len(index1.files), elapsedtime)


def clean(args):
    starttime = time.time()
    excludes = [fnmatch.translate('.pysync')]
    if args.excludes: excludes += map(fnmatch.translate, args.excludes)
    index = core.create_index(args.directory, excludes=excludes, archive_path=os.path.join(args.directory, '.pysync'))
    elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
    print 'Loaded index with {} files in {}.'.format(len(index.files), elapsedtime)

    hash_dict = defaultdict(list)
    for fd in index.files:
        hash_dict[fd.sha256].append(fd)
    for h, fds in hash_dict.iteritems():
        if len(fds) > 1:
            print '{} files with SHA256 {}:'.format(len(fds), h)
            for fd in fds:
                print '* {}'.format(fd.relpath)

def sync(args):
    starttime = time.time()
    excludes = [fnmatch.translate('.pysync')]
    if args.excludes: excludes += map(fnmatch.translate, args.excludes)
    index1 = core.create_index(args.source, excludes=excludes, archive_path=os.path.join(args.source, '.pysync'))
    elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
    print 'Loaded index with {} files in {}.'.format(len(index1.files), elapsedtime)

    index2 = core.create_index(args.target, excludes=excludes, archive_path=os.path.join(args.target, '.pysync'))
    elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
    print 'Loaded index with {} files in {}.'.format(len(index2.files), elapsedtime)

    changeset = index1.compare(index2)
    print '{} new files:'.format(len(changeset.new_files))
    for desc in changeset.new_files:
        print '\t{}'.format(desc.relpath)
    print '{} deleted files:'.format(len(changeset.removed_files))
    for desc in changeset.removed_files:
        print '\t{}'.format(desc.relpath)
    print '{} changed files:'.format(len(changeset.file_changes))
    for old, new in changeset.file_changes:
        print '\t{}'.format(old.relpath)

def main(argv):
    parser = argparse.ArgumentParser(prog='pysync')
    # parser.add_argument('--', dest='breaker', action='store_true')
    subparsers = parser.add_subparsers()

    indexparser = subparsers.add_parser('index')
    indexparser.add_argument('directory')
    indexparser.set_defaults(func=index)
    indexparser.add_argument('--excludes', metavar='pattern', nargs='+')

    indexparser = subparsers.add_parser('clean')
    indexparser.add_argument('directory')
    indexparser.set_defaults(func=clean)
    indexparser.add_argument('--excludes', metavar='pattern', nargs='+')

    syncparser = subparsers.add_parser('sync')
    syncparser.set_defaults(func=sync)
    syncparser.add_argument('source')
    syncparser.add_argument('target')
    syncparser.add_argument('--excludes', metavar='pattern', nargs='+')

    try:
        args = parser.parse_args(argv[1:])
    except StandardError as e:
        parser.print_help()
        sys.exit(1)
    args.func(args)
