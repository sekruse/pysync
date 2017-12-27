import hash, os, re, sqlite3, sys, time, datetime, difflib, shutil
from collections import defaultdict

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

    def compare(self, target):
        changeset = ChangeSet()
        target_index = target.create_path_index()

        # Match source to target files by name.
        new_files = defaultdict(list)
        for fd in self.files:
            if fd.relpath in target_index:
                target_fd = target_index[fd.relpath]
                if fd.size != target_fd.size or fd.sha256 != target_fd.sha256:
                    changeset.file_changes.append((fd, target_fd))
                del target_index[fd.relpath]
            else:
                new_files[fd.sha256].append(fd)

        # Find unmatched target files.
        deleted_files = defaultdict(list)
        for del_fd in target_index.itervalues():
            deleted_files[del_fd.sha256].append(del_fd)

        # Match unmatched files by content.
        for del_fds in deleted_files.values():
            sha256 = del_fds[0].sha256
            new_fds = new_files[sha256]
            if not new_fds: continue
            del new_files[sha256]
            del deleted_files[sha256]
            matches, remnant_new_fds, remnant_del_fds = fuzzy_match_names(new_fds, del_fds)
            for match in matches: changeset.file_moves.append(match)
            for fd in remnant_new_fds: changeset.new_files.append(fd)
            for fd in remnant_del_fds: changeset.removed_files.append(fd)

        # Find still unmatched source and target files.
        for new_fds in new_files.itervalues():
            changeset.new_files += new_fds
        for del_fds in deleted_files.itervalues():
            changeset.removed_files += del_fds
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

    def delete(self, fd):
        if not self.conn: self.open()
        self.conn.execute("""
            delete
            from file_descriptors
            where relpath = ?;
        """, (Archive._make_unicode(fd.relpath),))

    def insert(self, fd):
        if not self.conn: self.open()
        self.delete(fd)
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
        self.file_moves = []


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

def create_index(basepath, includes=[], excludes=[], archive=None):
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

def fuzzy_match_names(fds1, fds2):
    result = []
    matches = []
    remnant_fds1 = set(fds1)
    remnant_fds2 = set(fds2)
    for fd1 in fds1:
        for fd2 in fds2:
            matches.append((fd1, fd2, difflib.SequenceMatcher(None, fd1.relpath, fd2.relpath).ratio()))
    matches.sort(cmp=lambda a, b: a - b, key=lambda e: e[2], reverse=True)
    while remnant_fds1 and remnant_fds2:
        while True:
            fd1, fd2, ratio = matches.pop()
            if ratio == 0: break
            if fd1 in remnant_fds1 and fd2 in remnant_fds2:
                remnant_fds1.discard(fd1)
                remnant_fds2.discard(fd2)
                break
        result.append((fd1, fd2))
    return result, remnant_fds1, remnant_fds2

def copy(source_path, target_path, overwrite=False):
    if not overwrite and os.path.exists(target_path):
        raise OSError('Cannot copy {} to {}: target file exists'.format(source_path, target_path))
    if os.path.isdir(target_path):
        raise OSError('Cannot copy {} to {}: target is an existing directory'.format(source_path, target_path))
    target_dir = os.path.dirname(target_path)
    if not os.path.exists(target_dir): os.makedirs(target_dir)
    if overwrite: os.remove(target_path)
    shutil.copy(source_path, target_path)

def move(source_path, target_path, overwrite=False):
    if not overwrite and os.path.exists(target_path):
        raise OSError('Cannot move {} to {}: target file exists'.format(source_path, target_path))
    if os.path.isdir(target_path):
        raise OSError('Cannot move {} to {}: target is an existing directory'.format(source_path, target_path))
    target_dir = os.path.dirname(target_path)
    if not os.path.exists(target_dir): os.makedirs(target_dir)
    if overwrite: os.remove(target_path)
    os.rename(source_path, target_path)

def find_archive(path, archive_file='.pysync'):
    while True:
        if os.path.isdir(path):
            candidate = os.path.join(path, archive_file)
            if os.path.isfile(candidate): return candidate
        head, tail = os.path.split(path)
        if not tail: return None
        path = head
