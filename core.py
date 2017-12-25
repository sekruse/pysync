import hash, os, re, sqlite3, sys, time, datetime

class FileDescriptor:
    def __init__(self, relpath, mtime, size, sha256=None):
        self.relpath = relpath
        self.mtime = mtime
        self.size = size
        self.sha256 = sha256

    def __repr__(self):
        return 'FileDescriptor({}, mtime={}, size={}, sha256={})'.format(repr(self.relpath), repr(self.mtime), repr(self.size), repr(self.sha256))

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
        self.conn = None

    def open(self):
        self.conn = sqlite3.connect(self.path)
        self.conn.isolation_level = None
        self.conn.executescript("""
            create table if not exists file_descriptors (
                relpath text not null unique,
                size integer not null,
                mtime real not null,
                sha256 blob
            );
        """)
        self.conn.commit()
        return self.conn

    def load(self):
        if not self.conn: self.open()
        contents = dict()
        c = self.conn.cursor()
        for path, size, mtime, sha256 in c.execute('select * from file_descriptors;'):
            contents[path] = FileDescriptor(relpath, mtime, size, sha256)
        return contents

    def get(self, path):
        if not self.conn: self.open()
        row = self.conn.execute('''
            select size, mtime, sha256 from file_descriptors where relpath = ?;
        ''', (Archive._make_unicode(path),)).fetchone()
        return FileDescriptor(path, size=row[0], mtime=row[1], sha256=row[2]) if row else None

    def __getitem__(self, path):
        return self.get(path)

    def update(self, fd):
        if not self.conn: self.open()
        self.conn.execute("""
            update file_descriptors
            set size = ?, mtime = ?, sha256 = ?
            where relpath = ?;
        """, (fd.size, fd.mtime, fd.sha256, Archive._make_unicode(fd.relpath)))

    def insert(self, fd):
        if not self.conn: self.open()
        self.conn.execute("""
            insert into file_descriptors (relpath, size, mtime, sha256)
            values (?, ?, ?, ?);
        """, (Archive._make_unicode(fd.relpath), fd.size, fd.mtime, fd.sha256))

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    @staticmethod
    def _make_unicode(path):
        if type(path) == str: return path.decode('utf-8')
        elif type(path) == unicode: return path
        else: raise ValueError('Illegal path: {}'.format(repr(path)))

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

def create_index(basepath, includes=[], excludes=[], archive_path=None):
    archive = None
    try:
        if archive_path: archive = Archive(archive_path)
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
                    fd = create_descriptor(path, basepath)
                    old_fd = archive[fd.relpath] if archive else None
                    if not old_fd:
                        sys.stdout.write('Calculating SHA256 for {}... '.format(path))
                        starttime = time.time()
                        fd.sha256 = sha256 = hash.sha256(path)
                        sys.stdout.write('({})\n'.format(datetime.timedelta(seconds=time.time()-starttime)))
                        sys.stdout.flush()
                        if archive: archive.insert(fd)
                    elif old_fd.size != fd.size or old_fd.mtime != fd.mtime:
                        sys.stdout.write('Calculating SHA256 for {}... '.format(path))
                        starttime = time.time()
                        fd.sha256 = sha256 = hash.sha256(path)
                        sys.stdout.write('({})\n'.format(datetime.timedelta(seconds=time.time()-starttime)))
                        sys.stdout.flush()
                        if archive: archive.update(fd)
                    else:
                        fd.sha256 = old_fd.sha256
                    fileindex.files.append(fd)
            except (EnvironmentError, SystemError) as e:
                sys.stderr.write('Could not process {}: {}\n'.format(path, e))
                sys.stderr.flush()
    finally:
        if archive: archive.close()
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
