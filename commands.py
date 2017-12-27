import time, sys, os, datetime, argparse, fnmatch, core, re
from collections import defaultdict

def get_archive(basepath):
    '''Get or create a suitable `Archive` for the `basepath`.'''
    # TODO: Handle relative paths.
    # archive_path = core.find_archive(basepath)
    return core.Archive(os.path.join(basepath, '.pysync'))

def index(args):
    starttime = time.time()
    excludes = [fnmatch.translate('.pysync')]
    if args.excludes: excludes += map(fnmatch.translate, args.excludes)
    archive = get_archive(args.directory)
    index1 = core.create_index(args.directory, excludes=excludes, archive=archive)
    elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
    print 'Loaded index with {} files in {}.'.format(len(index1.files), elapsedtime)


def clean(args):
    starttime = time.time()
    excludes = [fnmatch.translate('.pysync')]
    if args.excludes: excludes += map(fnmatch.translate, args.excludes)
    archive = get_archive(args.directory)
    index = core.create_index(args.directory, excludes=excludes, archive=archive)
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
    source_archive = get_archive(args.source)
    source_index = core.create_index(args.source, excludes=excludes, archive=source_archive)
    elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
    print 'Loaded index with {} files in {}.'.format(len(source_index.files), elapsedtime)

    target_archive = get_archive(args.target)
    target_index = core.create_index(args.target, excludes=excludes, archive=target_archive)
    elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
    print 'Loaded index with {} files in {}.'.format(len(target_index.files), elapsedtime)

    changeset = source_index.compare(target_index)

    print 'Enter command:'
    apply_regex = re.compile('apply (?P<location>source|target) (?P<pattern>.*)(?:[\s\n\r]*)')
    revert_regex = re.compile('revert (?P<location>source|target) (?P<pattern>.*)(?:[\s\n\r]*)')
    ignore_regex = re.compile('ignore (?P<location>source|target) (?P<pattern>.*)(?:[\s\n\r]*)')
    while True:
        try:
            line = raw_input('> ')
        except EOFError:
            line = ''
        if not line or line == 'exit':
            break

        # Command: show
        if line == 'show':
            print '{} new files:'.format(len(changeset.new_files))
            for desc in changeset.new_files:
                print '\t{}'.format(desc.relpath)
            print '{} deleted files:'.format(len(changeset.removed_files))
            for desc in changeset.removed_files:
                print '\t{}'.format(desc.relpath)
            print '{} changed files:'.format(len(changeset.file_changes))
            for new, old in changeset.file_changes:
                print '\t{}'.format(old.relpath)
            print '{} moved files:'.format(len(changeset.file_moves))
            for new, old in changeset.file_moves:
                print '\t{} -> {}'.format(old.relpath, new.relpath)
            continue

        # Command: apply
        match = apply_regex.match(line)
        if match:
            location = match.group('location')
            pattern = match.group('pattern')

            # Apply new files.
            if location == 'source':
                copy_fds = [fd for fd in changeset.new_files if fnmatch.fnmatch(fd.relpath, pattern)]
                for fd in copy_fds:
                    source_path = os.path.join(args.source, fd.relpath)
                    target_path = os.path.join(args.target, fd.relpath)
                    print 'Copying {} to {}...'.format(source_path, target_path)
                    changeset.new_files.remove(fd)
                    if not args.dryrun:
                        try:
                            core.copy(source_path, target_path, overwrite=False)
                            stat = os.stat(target_path)
                            new_fd = core.FileDescriptor(relpath=fd.relpath, mtime=stat.st_mtime, size=stat.st_size, sha256=fd.sha256)
                            target_archive.insert(new_fd)
                        except EnvironmentError as e:
                            sys.stderr.write('Could not copy {} to {}: {}\n', source_path, target_path, e)
                            sys.stderr.flush()

            # Apply deleted files.
            if location == 'target':
                del_fds = [fd for fd in changeset.removed_files if fnmatch.fnmatch(fd.relpath, pattern)]
                for fd in del_fds:
                    path = os.path.join(args.target, fd.relpath)
                    print 'Deleting {}...'.format(path)
                    changeset.removed_files.remove(fd)
                    if not args.dryrun:
                        try:
                            os.remove(path)
                            target_archive.delete(fd)
                        except EnvironmentError as e:
                            sys.stderr.write('Could not delete: {}\n', path, e)
                            sys.stderr.flush()

            # Apply file changes.
            if location == 'source':
                match_index = 0
            elif location == 'target':
                match_index = 1
            changes = [fds for fds in changeset.file_changes if fnmatch.fnmatch(fds[match_index].relpath, pattern)]
            for source_fd, target_fd in changes:
                source_path = os.path.join(args.source, source_fd.relpath)
                target_path = os.path.join(args.target, target_fd.relpath)
                print 'Copying {} to {}...'.format(source_path, target_path)
                changeset.file_changes.remove((source_fd, target_fd))
                if not args.dryrun:
                    try:
                        core.copy(source_path, target_path, overwrite=True)
                        stat = os.stat(target_path)
                        new_fd = core.FileDescriptor(relpath=target_fd.relpath, mtime=stat.st_mtime, size=stat.st_size, sha256=source_fd.sha256)
                        target_archive.update(new_fd)
                    except EnvironmentError as e:
                        sys.stderr.write('Could not copy {} to {}: {}\n', source_path, target_path, e)
                        sys.stderr.flush()

            # Apply file moves.
            if location == 'source':
                match_index = 0
            elif location == 'target':
                match_index = 1
            changes = [fds for fds in changeset.file_moves if fnmatch.fnmatch(fds[match_index].relpath, pattern)]
            for source_fd, target_fd in changes:
                from_path = os.path.join(args.target, target_fd.relpath)
                to_path = os.path.join(args.target, source_fd.relpath)
                print 'Moving {} to {}...'.format(from_path, to_path)
                changeset.file_moves.remove((source_fd, target_fd))
                if not args.dryrun:
                    try:
                        core.move(from_path, to_path, overwrite=False)
                        stat = os.stat(to_path)
                        new_fd = core.FileDescriptor(relpath=source_fd.relpath, mtime=stat.st_mtime, size=stat.st_size, sha256=target_fd.sha256)
                        target_archive.delete(target_fd)
                        target_archive.insert(new_fd)
                    except EnvironmentError as e:
                        sys.stderr.write('Could not move {} to {}: {}\n', from_path, to_path, e)
                        sys.stderr.flush()
            continue

        # Command: revert
        match = revert_regex.match(line)
        if match:
            location = match.group('location')
            pattern = match.group('pattern')

            # Revert deleted files.
            if location == 'target':
                copy_fds = [fd for fd in changeset.removed_files if fnmatch.fnmatch(fd.relpath, pattern)]
                for fd in copy_fds:
                    source_path = os.path.join(args.source, fd.relpath)
                    target_path = os.path.join(args.target, fd.relpath)
                    print 'Copying {} to {}...'.format(target_path, source_path)
                    changeset.removed_files.remove(fd)
                    if not args.dryrun:
                        try:
                            core.copy(target_path, source_path, overwrite=False)
                            stat = os.stat(source_path)
                            new_fd = core.FileDescriptor(relpath=fd.relpath, mtime=stat.st_mtime, size=stat.st_size, sha256=fd.sha256)
                            source_archive.insert(new_fd)
                        except EnvironmentError as e:
                            sys.stderr.write('Could not copy {} to {}: {}\n', target_path, source_path, e)
                            sys.stderr.flush()

            # Revert new files.
            if location == 'source':
                del_fds = [fd for fd in changeset.new_files if fnmatch.fnmatch(fd.relpath, pattern)]
                for fd in del_fds:
                    path = os.path.join(args.source, fd.relpath)
                    print 'Deleting {}...'.format(path)
                    changeset.new_files.remove(fd)
                    if not args.dryrun:
                        try:
                            os.remove(path)
                            source_archive.delete(fd)
                        except EnvironmentError as e:
                            sys.stderr.write('Could not delete {}: {}\n', path, e)
                            sys.stderr.flush()

            # Apply file changes.
            if location == 'source':
                match_index = 0
            elif location == 'target':
                match_index = 1
            changes = [fds for fds in changeset.file_changes if fnmatch.fnmatch(fds[match_index].relpath, pattern)]
            for source_fd, target_fd in changes:
                source_path = os.path.join(args.source, source_fd.relpath)
                target_path = os.path.join(args.target, target_fd.relpath)
                print 'Copying {} to {}...'.format(target_path, source_path)
                changeset.file_changes.remove((source_fd, target_fd))
                if not args.dryrun:
                    try:
                        core.copy(target_path, source_path, overwrite=True)
                        stat = os.stat(source_path)
                        new_fd = core.FileDescriptor(relpath=source_fd.relpath, mtime=stat.st_mtime, size=stat.st_size, sha256=target_fd.sha256)
                        source_archive.update(new_fd)
                    except EnvironmentError as e:
                        sys.stderr.write('Could not copy {} to {}: {}\n', target_path, source_path, e)
                        sys.stderr.flush()

            # Apply file moves.
            if location == 'source':
                match_index = 0
            elif location == 'target':
                match_index = 1
            changes = [fds for fds in changeset.file_moves if fnmatch.fnmatch(fds[match_index].relpath, pattern)]
            for source_fd, target_fd in changes:
                from_path = os.path.join(args.source, source_fd.relpath)
                to_path = os.path.join(args.source, target_fd.relpath)
                print 'Moving {} to {}...'.format(from_path, to_path)
                changeset.file_moves.remove((source_fd, target_fd))
                if not args.dryrun:
                    try:
                        core.move(from_path, to_path, overwrite=False)
                        stat = os.stat(to_path)
                        new_fd = core.FileDescriptor(relpath=target_fd.relpath, mtime=stat.st_mtime, size=stat.st_size, sha256=source_fd.sha256)
                        source_archive.delete(source_fd)
                        source_archive.insert(new_fd)
                    except EnvironmentError as e:
                        sys.stderr.write('Could not move {} to {}: {}\n', from_path, to_path, e)
                        sys.stderr.flush()
            continue

        print 'Unknown command.'

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
    syncparser.add_argument('--dry-run', dest='dryrun', action='store_const', const=True, default=False)

    try:
        args = parser.parse_args(argv[1:])
    except StandardError as e:
        parser.print_help()
        sys.exit(1)
    args.func(args)
