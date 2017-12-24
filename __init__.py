import os, re, sys, hash, sqlite3
from collections import defaultdict

__all__ = ['hash']

class FileDescriptor:
    def __init__(self, relpath, mtime, size, sha256=None):
        self.relpath = relpath
        self.mtime = mtime
        self.size = size
        self.sha256 = sha256

    def __repr__(self):
        return 'FileDescriptor({}, mtime={}, size={}, sha256={})'.format(self.relpath, self.mtime, self.size, self.sha256)

class FileIndex:
    def __init__(self, basepath):
        self.basepath = basepath
        self.files = []

    def init_hashes(archive):
        archived_descriptors = dict()
        if archive and os.isfile(os.path.join(self.basepath, archive)):
            sqlite3

    def create_path_index(self):
        return dict([(d.relpath, d) for d in self.files])

    def compare(self, other):
        changeset = ChangeSet()
        other_index = other.create_path_index()
        deleted_files = []
        for f in self.files:
            if f.relpath in other_index:
                other_f = other_index[f.relpath]
                if f.size != other_f.size or f.sha256 != other_f.sha256:
                    changeset.file_changes.append((f, other_f))
                del other_index[f.relpath]
            else:
                deleted_files.append(f)
        new_files = defaultdict(list)
        for new_f in other_index.itervalues():
            new_files[new_f.sha256].append(new_f)
        for deleted_f in deleted_files:
            new_fs = new_files[deleted_f.sha256]
            if new_fs:
                changeset.file_changes.append(deleted, new_fs.pop())
            else:
                changeset.deleted_files.append(deleted_f)
        for new_fs in new_files.itervalues():
            changeset.new_files += new_fs
        return changeset

class Archive:
    def __init__(self, path):
        self.path = path

    def open(self):
        conn = sqlite3.connect(self.path)
        conn.executescript("""
            create table if not exists file_descriptors (
                relpath text not null unique,
                size integer not null,
                mtime real not null,
                sha256 blob
            );
        """)
        conn.commit()
        return conn

    def load(self):
        contents = dict()
        with self.open() as conn:
            c = conn.cursor()
            for path, size, mtime, sha256 in c.execute('select * from file_descriptors;'):
                contents[path] = FileDescriptor(relpath, mtime, size, sha256)
        return contents


class ChangeSet:
    def __init__(self):
        self.removed_files = []
        self.new_files = []
        self.file_changes = []


class FileFilter:
    def __init__(self, includes=[], excludes=[]):
        self.includes = [re.compile(pattern) for pattern in includes]
        self.excludes = [re.compile(pattern) for pattern in excludes]
    def is_filtered(self, path):
        head, tail = os.path.split(path)
        for include in self.includes:
            if include.match(tail): return False
        for exclude in self.excludes:
            if exclude.match(tail): return True
        return False


def open_archive(path):
    if not path: return None
    conn = sqlite3.connect(path)
    conn.isolation_level = None
    conn.executescript("""
        create table if not exists file_descriptors (
            relpath text not null unique,
            size integer not null,
            mtime real not null,
            sha256 blob
        );
    """)
    return conn

def create_index(basepath, includes=[], excludes=[], archive_path=None):
    archive_connection = None
    try:
        archive_connection = open_archive(archive_path)
        basepath = os.path.normpath(basepath)
        fileindex = FileIndex(basepath)
        filefilter = FileFilter(includes=includes, excludes=excludes)
        # Read the files in the directory.
        queue = [basepath]
        while len(queue) > 0:
            path = queue.pop()
            if filefilter.is_filtered(path): continue
            try:
                if os.path.isdir(path):
                    for child in os.listdir(path): queue.append(os.path.join(path, child))
                else:
                    descriptor = create_descriptor(path, basepath)
                    row = archive_connection.execute('''
                        select size, mtime, sha256 from file_descriptors where relpath = ?;
                    ''', (descriptor.relpath,)).fetchone() if archive_connection else None
                    if row is None:
                        sys.stdout.write('Calculating SHA256 for {}... '.format(path))
                        starttime = time.time()
                        descriptor.sha256 = sha256 = hash.sha256(path)
                        sys.stdout.write('({})\n'.format(datetime.timedelta(seconds=time.time()-starttime)))
                        sys.stdout.flush()
                        if archive_connection is not None:
                            archive_connection.execute("""
                                insert into file_descriptors (relpath, size, mtime, sha256)
                                values (?, ?, ?, ?);
                            """, (descriptor.relpath.encode('utf-8'), descriptor.size, descriptor.mtime, descriptor.sha256))
                    elif row[0] != descriptor.size or row[1] != descriptor.mtime:
                        sys.stdout.write('Calculating SHA256 for {}... '.format(path))
                        starttime = time.time()
                        descriptor.sha256 = sha256 = hash.sha256(path)
                        sys.stdout.write('({})\n'.format(datetime.timedelta(seconds=time.time()-starttime)))
                        sys.stdout.flush()
                        archive_connection.execute("""
                            update file_descriptors
                            set size = ?, mtime = ?, sha256 = ?
                            where relpath = ?;
                        """, (descriptor.size, descriptor.mtime, descriptor.sha256, descriptor.relpath.encode('utf-8')))
                    else:
                        descriptor.sha256 = row[2]
                    fileindex.files.append(descriptor)
                    # print descriptor
            except StandardError as e:
                sys.stderr.write('Could not process {}: {}\n'.format(path, e))
                sys.stderr.flush()
    finally:
        if archive_connection: archive_connection.close()
    return fileindex

def split_path(path):
    path = os.path.normpath(path)
    l = []
    while True:
        head, tail = os.path.split(path)
        if len(tail) > 0:
            l.append(tail)
            path = head
        else:
            l.append(head)
            break
    l.reverse()
    return l

def create_descriptor(path, basepath):
    if not os.path.isfile(path): raise ValueError('Not a file: {}'.format(path))
    stat = os.stat(path)
    return FileDescriptor(relpath=os.path.relpath(path, basepath), mtime=stat.st_mtime, size=stat.st_size)

if __name__ == '__main__':
    import time, sys, datetime, argparse, fnmatch

    def index(args):
        starttime = time.time()
        excludes = [fnmatch.translate('.pysync')]
        if args.excludes: excludes += map(fnmatch.translate, args.excludes)
        index1 = create_index(args.directory, excludes=excludes, archive_path=os.path.join(args.directory, '.pysync'))
        elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
        print 'Created index with {} files in {}.'.format(len(index1.files), elapsedtime)

    def sync(args):
        starttime = time.time()
        excludes = [fnmatch.translate('.pysync')]
        if args.excludes: excludes += map(fnmatch.translate, args.excludes)
        index1 = create_index(args.source, excludes=excludes, archive_path=os.path.join(args.source, '.pysync'))
        elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
        print 'Created index with {} files in {}.'.format(len(index1.files), elapsedtime)

        index2 = create_index(args.target, excludes=excludes, archive_path=os.path.join(args.target, '.pysync'))
        elapsedtime = datetime.timedelta(seconds=time.time() - starttime)
        print 'Created index with {} files in {}.'.format(len(index2.files), elapsedtime)

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

    parser = argparse.ArgumentParser(prog='pysync')
    # parser.add_argument('--', dest='breaker', action='store_true')
    subparsers = parser.add_subparsers()

    indexparser = subparsers.add_parser('index')
    indexparser.add_argument('directory')
    indexparser.set_defaults(func=index)
    indexparser.add_argument('--excludes', metavar='pattern', nargs='+')

    syncparser = subparsers.add_parser('sync')
    syncparser.set_defaults(func=sync)
    syncparser.add_argument('source')
    syncparser.add_argument('target')
    syncparser.add_argument('--excludes', metavar='pattern', nargs='+')

    try:
        args = parser.parse_args(sys.argv[1:])
    except StandardError as e:
        parser.print_help()
        sys.exit(1)
    args.func(args)
