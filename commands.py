import time, sys, os, datetime, argparse, fnmatch, core, re
from collections import defaultdict

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
    for h, fds in hash_dict.items():
        if len(fds) < 2: del hash_dict[h]

    print 'Detected {} duplicate groups.'.format(len(hash_dict))
    print 'Enter command:'
    keep_regex = re.compile('keep (?P<pattern>.*)(?:[\s\n\r]*)')
    delete_regex = re.compile('delete (?P<pattern>.*)(?:[\s\n\r]*)')
    ignore_regex = re.compile('ignore (?P<pattern>.*)(?:[\s\n\r]*)')
    while True:
        try:
            line = raw_input('> ')
        except EOFError:
            line = ''
        if not line or line == 'exit':
            break

        if line == 'show':
            for h, fds in hash_dict.iteritems():
                print '{} files with SHA256 {}:'.format(len(fds), h)
                for fd in fds:
                    print '* {}'.format(fd.relpath)
            continue

        match = keep_regex.match(line)
        if match:
            pattern = match.group('pattern')
            for h, fds in hash_dict.items():
                delete_fds = [fd for fd in fds if not fnmatch.fnmatch(fd.relpath, pattern)]
                if len(delete_fds) < len(fds):
                    for delete_fd in delete_fds:
                        del_path = os.path.join(args.directory, delete_fd.relpath)
                        print 'Deleting {}...'.format(del_path)
                        if not args.dryrun:
                            try: os.remove(del_path)
                            except OSError:
                                sys.stderr.write('Could not remove {}.\n'.format(del_path))
                                sys.stderr.flush()
                        fds.remove(delete_fd)
                    if len(fds) < 2: del hash_dict[h]
            continue

        match = ignore_regex.match(line)
        if match:
            pattern = match.group('pattern')
            for h, fds in hash_dict.items():
                for fd in fds[:]:
                    if fnmatch.fnmatch(fd.relpath, pattern):
                        print 'Ignoring {}...'.format(fd.relpath)
                        fds.remove(fd)
                if len(fds) < 2: del hash_dict[h]
            continue

        match = delete_regex.match(line)
        if match:
            pattern = match.group('pattern')
            for h, fds in hash_dict.items():
                delete_fds = [fd for fd in fds if fnmatch.fnmatch(fd.relpath, pattern)]
                if len(delete_fds) < len(fds):
                    for delete_fd in delete_fds:
                        del_path = os.path.join(args.directory, delete_fd.relpath)
                        print 'Deleting {}...'.format(del_path)
                        if not args.dryrun:
                            try: os.remove(del_path)
                            except OSError:
                                sys.stderr.write('Could not remove {}.\n'.format(del_path))
                                sys.stderr.flush()
                        fds.remove(delete_fd)
                    if len(fds) < 2: del hash_dict[h]
            continue

        print 'Unknown command.'


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
    indexparser.add_argument('--dry-run', dest='dryrun', action='store_const', const=True, default=False)

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
