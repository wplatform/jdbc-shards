/*
 * Copyright 2014-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wplatform.ddal.util;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;

/**
 * A path to a file. It similar to the Java 7 <code>java.nio.file.Path</code>,
 * but simpler, and works with older versions of Java. It also implements the
 * relevant methods found in <code>java.nio.file.FileSystem</code> and
 * <code>FileSystems</code>
 */
public abstract class FilePath {

    private static FilePath defaultProvider = new FilePathDisk();

    private static Map<String, FilePath> providers = New.hashMap();

    /**
     * The prefix for temporary files.
     */
    private static String tempRandom;
    private static long tempSequence;

    /**
     * The complete path (which may be absolute or relative, depending on the
     * file system).
     */
    protected String name;

    /**
     * Get the file path object for the given path. Windows-style '\' is
     * replaced with '/'.
     *
     * @param path the path
     * @return the file path object
     */
    public static FilePath get(String path) {
        path = path.replace('\\', '/');
        int index = path.indexOf(':');
        if (index < 2) {
            // use the default provider if no prefix or
            // only a single character (drive name)
            return defaultProvider.getPath(path);
        }
        String scheme = path.substring(0, index);
        FilePath p = providers.get(scheme);
        if (p == null) {
            // provider not found - use the default
            p = defaultProvider;
        }
        return p.getPath(path);
    }

    /**
     * Register a file provider.
     *
     * @param provider the file provider
     */
    public static void register(FilePath provider) {
        providers.put(provider.getScheme(), provider);
    }

    /**
     * Unregister a file provider.
     *
     * @param provider the file provider
     */
    public static void unregister(FilePath provider) {
        providers.remove(provider.getScheme());
    }

    /**
     * Get the next temporary file name part (the part in the middle).
     *
     * @param newRandom if the random part of the filename should change
     * @return the file name part
     */
    protected static synchronized String getNextTempFileNamePart(boolean newRandom) {
        if (newRandom || tempRandom == null) {
            tempRandom = MathUtils.randomInt(Integer.MAX_VALUE) + ".";
        }
        return tempRandom + tempSequence++;
    }

    /**
     * Get the size of a file in bytes
     *
     * @return the size in bytes
     */
    public abstract long size();

    /**
     * Rename a file if this is allowed.
     *
     * @param newName       the new fully qualified file name
     * @param atomicReplace whether the move should be atomic, and the target
     *                      file should be replaced if it exists and replacing is possible
     */
    public abstract void moveTo(FilePath newName, boolean atomicReplace);

    /**
     * Create a new file.
     *
     * @return true if creating was successful
     */
    public abstract boolean createFile();

    /**
     * Checks if a file exists.
     *
     * @return true if it exists
     */
    public abstract boolean exists();

    /**
     * Delete a file or directory if it exists. Directories may only be deleted
     * if they are empty.
     */
    public abstract void delete();

    /**
     * List the files and directories in the given directory.
     *
     * @return the list of fully qualified file names
     */
    public abstract List<FilePath> newDirectoryStream();

    /**
     * Normalize a file name.
     *
     * @return the normalized file name
     */
    public abstract FilePath toRealPath();

    /**
     * Get the parent directory of a file or directory.
     *
     * @return the parent directory name
     */
    public abstract FilePath getParent();

    /**
     * Check if it is a file or a directory.
     *
     * @return true if it is a directory
     */
    public abstract boolean isDirectory();

    /**
     * Check if the file name includes a path.
     *
     * @return if the file name is absolute
     */
    public abstract boolean isAbsolute();

    /**
     * Get the last modified date of a file
     *
     * @return the last modified date
     */
    public abstract long lastModified();

    /**
     * Check if the file is writable.
     *
     * @return if the file is writable
     */
    public abstract boolean canWrite();

    /**
     * Create a directory (all required parent directories already exist).
     */
    public abstract void createDirectory();

    /**
     * Get the file or directory name (the last element of the path).
     *
     * @return the last element of the path
     */
    public String getName() {
        int idx = Math.max(name.indexOf(':'), name.lastIndexOf('/'));
        return idx < 0 ? name : name.substring(idx + 1);
    }

    /**
     * Create an output stream to write into the file.
     *
     * @param append if true, the file will grow, if false, the file will be
     *               truncated first
     * @return the output stream
     */
    public abstract OutputStream newOutputStream(boolean append) throws IOException;

    /**
     * Open a random access file object.
     *
     * @param mode the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     */
    public abstract FileChannel open(String mode) throws IOException;

    /**
     * Create an input stream to read from the file.
     *
     * @return the input stream
     */
    public abstract InputStream newInputStream() throws IOException;

    /**
     * Disable the ability to write.
     *
     * @return true if the call was successful
     */
    public abstract boolean setReadOnly();

    /**
     * Create a new temporary file.
     *
     * @param suffix       the suffix
     * @param deleteOnExit if the file should be deleted when the virtual
     *                     machine exists
     * @param inTempDir    if the file should be stored in the temporary directory
     * @return the name of the created file
     */
    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        while (true) {
            FilePath p = getPath(name + getNextTempFileNamePart(false) + suffix);
            if (p.exists() || !p.createFile()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            p.open("rw").close();
            return p;
        }
    }

    /**
     * Get the string representation. The returned string can be used to
     * construct a new object.
     *
     * @return the path as a string
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * Get the scheme (prefix) for this file provider. This is similar to
     * <code>java.nio.file.spi.FileSystemProvider.getScheme</code>.
     *
     * @return the scheme
     */
    public abstract String getScheme();

    /**
     * Convert a file to a path. This is similar to
     * <code>java.nio.file.spi.FileSystemProvider.getPath</code>, but may return
     * an object even if the scheme doesn't match in case of the the default
     * file provider.
     *
     * @param path the path
     * @return the file path object
     */
    public abstract FilePath getPath(String path);

    /**
     * Get the unwrapped file name (without wrapper prefixes if wrapping /
     * delegating file systems are used).
     *
     * @return the unwrapped path
     */
    public FilePath unwrap() {
        return this;
    }

    /**
     * This file system stores files on disk. This is the most common file
     * system.
     */
    public static class FilePathDisk extends FilePath {

        private static final String CLASSPATH_PREFIX = "classpath:";

        /**
         * Translate the file name to the native format. This will replace '\'
         * with '/' and expand the home directory ('~').
         *
         * @param fileName the file name
         * @return the native file name
         */
        protected static String translateFileName(String fileName) {
            fileName = fileName.replace('\\', '/');
            if (fileName.startsWith("file:")) {
                fileName = fileName.substring("file:".length());
            }
            return expandUserHomeDirectory(fileName);
        }

        /**
         * Expand '~' to the user home directory. It is only be expanded if the
         * '~' stands alone, or is followed by '/' or '\'.
         *
         * @param fileName the file name
         * @return the native file name
         */
        public static String expandUserHomeDirectory(String fileName) {
            if (fileName.startsWith("~") && (fileName.length() == 1 || fileName.startsWith("~/"))) {
                String userDir = SysProperties.USER_HOME;
                fileName = userDir + fileName.substring(1);
            }
            return fileName;
        }

        private static void wait(int i) {
            if (i == 8) {
                System.gc();
            }
            try {
                // sleep at most 256 ms
                long sleep = Math.min(256, i * i);
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        private static boolean canWriteInternal(File file) {
            try {
                if (!file.canWrite()) {
                    return false;
                }
            } catch (Exception e) {
                // workaround for GAE which throws a
                // java.security.AccessControlException
                return false;
            }
            // File.canWrite() does not respect windows user permissions,
            // so we must try to open it using the mode "rw".
            // See also
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4420020
            RandomAccessFile r = null;
            try {
                r = new RandomAccessFile(file, "rw");
                return true;
            } catch (FileNotFoundException e) {
                return false;
            } finally {
                if (r != null) {
                    try {
                        r.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }

        /**
         * Call the garbage collection and run finalization. This close all
         * files that were not closed, and are no longer referenced.
         */
        static void freeMemoryAndFinalize() {
            IOUtils.trace("freeMemoryAndFinalize", null, null);
            Runtime rt = Runtime.getRuntime();
            long mem = rt.freeMemory();
            for (int i = 0; i < 16; i++) {
                rt.gc();
                long now = rt.freeMemory();
                rt.runFinalization();
                if (now == mem) {
                    break;
                }
                mem = now;
            }
        }

        @Override
        public FilePathDisk getPath(String path) {
            FilePathDisk p = new FilePathDisk();
            p.name = translateFileName(path);
            return p;
        }

        @Override
        public long size() {
            return new File(name).length();
        }

        @Override
        public void moveTo(FilePath newName, boolean atomicReplace) {
            File oldFile = new File(name);
            File newFile = new File(newName.name);
            if (oldFile.getAbsolutePath().equals(newFile.getAbsolutePath())) {
                return;
            }
            if (!oldFile.exists()) {
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name + " (not found)",
                        newName.name);
            }
            // Java 7: use java.nio.file.Files.move(Path source, Path target,
            // CopyOption... options)
            // with CopyOptions "REPLACE_EXISTING" and "ATOMIC_MOVE".
            if (atomicReplace) {
                boolean ok = oldFile.renameTo(newFile);
                if (ok) {
                    return;
                }
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name,
                        newName.name);
            }
            if (newFile.exists()) {
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name,
                        newName + " (exists)");
            }
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                IOUtils.trace("rename", name + " >" + newName, null);
                boolean ok = oldFile.renameTo(newFile);
                if (ok) {
                    return;
                }
                wait(i);
            }
            throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                    name, newName.name);
        }

        @Override
        public boolean createFile() {
            File file = new File(name);
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                try {
                    return file.createNewFile();
                } catch (IOException e) {
                    // 'access denied' is really a concurrent access problem
                    wait(i);
                }
            }
            return false;
        }

        @Override
        public boolean exists() {
            return new File(name).exists();
        }

        @Override
        public void delete() {
            File file = new File(name);
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                IOUtils.trace("delete", name, null);
                boolean ok = file.delete();
                if (ok || !file.exists()) {
                    return;
                }
                wait(i);
            }
            throw DbException.get(ErrorCode.FILE_DELETE_FAILED_1, name);
        }

        @Override
        public List<FilePath> newDirectoryStream() {
            ArrayList<FilePath> list = New.arrayList();
            File f = new File(name);
            try {
                String[] files = f.list();
                if (files != null) {
                    String base = f.getCanonicalPath();
                    if (!base.endsWith(SysProperties.FILE_SEPARATOR)) {
                        base += SysProperties.FILE_SEPARATOR;
                    }
                    for (int i = 0, len = files.length; i < len; i++) {
                        list.add(getPath(base + files[i]));
                    }
                }
                return list;
            } catch (IOException e) {
                throw DbException.convertIOException(e, name);
            }
        }

        @Override
        public boolean canWrite() {
            return canWriteInternal(new File(name));
        }

        @Override
        public boolean setReadOnly() {
            File f = new File(name);
            return f.setReadOnly();
        }

        @Override
        public FilePathDisk toRealPath() {
            try {
                String fileName = new File(name).getCanonicalPath();
                return getPath(fileName);
            } catch (IOException e) {
                throw DbException.convertIOException(e, name);
            }
        }

        @Override
        public FilePath getParent() {
            String p = new File(name).getParent();
            return p == null ? null : getPath(p);
        }

        @Override
        public boolean isDirectory() {
            return new File(name).isDirectory();
        }

        @Override
        public boolean isAbsolute() {
            return new File(name).isAbsolute();
        }

        @Override
        public long lastModified() {
            return new File(name).lastModified();
        }

        @Override
        public void createDirectory() {
            File dir = new File(name);
            for (int i = 0; i < SysProperties.MAX_FILE_RETRY; i++) {
                if (dir.exists()) {
                    if (dir.isDirectory()) {
                        return;
                    }
                    throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, name
                            + " (a file with this name already exists)");
                } else if (dir.mkdir()) {
                    return;
                }
                wait(i);
            }
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1, name);
        }

        @Override
        public OutputStream newOutputStream(boolean append) throws IOException {
            try {
                File file = new File(name);
                File parent = file.getParentFile();
                if (parent != null) {
                    FileUtils.createDirectories(parent.getAbsolutePath());
                }
                FileOutputStream out = new FileOutputStream(name, append);
                IOUtils.trace("openFileOutputStream", name, out);
                return out;
            } catch (IOException e) {
                freeMemoryAndFinalize();
                return new FileOutputStream(name);
            }
        }

        @Override
        public InputStream newInputStream() throws IOException {
            int index = name.indexOf(':');
            if (index > 1 && index < 20) {
                // if the ':' is in position 1, a windows file access is
                // assumed:
                // C:.. or D:, and if the ':' is not at the beginning, assume
                // its a
                // file name with a colon
                if (name.startsWith(CLASSPATH_PREFIX)) {
                    String fileName = name.substring(CLASSPATH_PREFIX.length());
                    if (!fileName.startsWith("/")) {
                        fileName = "/" + fileName;
                    }
                    InputStream in = getClass().getResourceAsStream(fileName);
                    if (in == null) {
                        in = Thread.currentThread().getContextClassLoader()
                                .getResourceAsStream(fileName);
                    }
                    if (in == null) {
                        throw new FileNotFoundException("resource " + fileName);
                    }
                    return in;
                }
                // otherwise an URL is assumed
                URL url = new URL(name);
                InputStream in = url.openStream();
                return in;
            }
            FileInputStream in = new FileInputStream(name);
            IOUtils.trace("openFileInputStream", name, in);
            return in;
        }

        @Override
        public FileChannel open(String mode) throws IOException {
            FileDisk f;
            try {
                f = new FileDisk(name, mode);
                IOUtils.trace("open", name, f);
            } catch (IOException e) {
                freeMemoryAndFinalize();
                try {
                    f = new FileDisk(name, mode);
                } catch (IOException e2) {
                    throw e;
                }
            }
            return f;
        }

        @Override
        public String getScheme() {
            return "file";
        }

        @Override
        public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
                throws IOException {
            String fileName = name + ".";
            String prefix = new File(fileName).getName();
            File dir;
            if (inTempDir) {
                dir = new File(System.getProperty("java.io.tmpdir", "."));
            } else {
                dir = new File(fileName).getAbsoluteFile().getParentFile();
            }
            FileUtils.createDirectories(dir.getAbsolutePath());
            while (true) {
                File f = new File(dir, prefix + getNextTempFileNamePart(false) + suffix);
                if (f.exists() || !f.createNewFile()) {
                    // in theory, the random number could collide
                    getNextTempFileNamePart(true);
                    continue;
                }
                if (deleteOnExit) {
                    try {
                        f.deleteOnExit();
                    } catch (Throwable e) {
                        // sometimes this throws a NullPointerException
                        // at
                        // java.io.DeleteOnExitHook.add(DeleteOnExitHook.java:33)
                        // we can ignore it
                    }
                }
                return get(f.getCanonicalPath());
            }
        }

    }


    /**
     * The base class for file implementations.
     */
    public static abstract class FileBase extends FileChannel {

        @Override
        public abstract long size() throws IOException;

        @Override
        public abstract long position() throws IOException;

        @Override
        public abstract FileChannel position(long newPosition) throws IOException;

        @Override
        public abstract int read(ByteBuffer dst) throws IOException;

        @Override
        public abstract int write(ByteBuffer src) throws IOException;

        @Override
        public synchronized int read(ByteBuffer dst, long position)
                throws IOException {
            long oldPos = position();
            position(position);
            int len = read(dst);
            position(oldPos);
            return len;
        }

        @Override
        public synchronized int write(ByteBuffer src, long position)
                throws IOException {
            long oldPos = position();
            position(position);
            int len = write(src);
            position(oldPos);
            return len;
        }

        @Override
        public abstract FileChannel truncate(long size) throws IOException;

        @Override
        public void force(boolean metaData) throws IOException {
            // ignore
        }

        @Override
        protected void implCloseChannel() throws IOException {
            // ignore
        }

        @Override
        public FileLock lock(long position, long size, boolean shared)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length)
                throws IOException {
            throw new UnsupportedOperationException();
        }

    }


    /**
     * Uses java.io.RandomAccessFile to access a file.
     */
    private class FileDisk extends FileBase {

        private final RandomAccessFile file;
        private final String name;
        private final boolean readOnly;

        FileDisk(String fileName, String mode) throws FileNotFoundException {
            this.file = new RandomAccessFile(fileName, mode);
            this.name = fileName;
            this.readOnly = mode.equals("r");
        }

        @Override
        public void force(boolean metaData) throws IOException {
            String m = SysProperties.SYNC_METHOD;
            if ("".equals(m)) {
                // do nothing
            } else if ("sync".equals(m)) {
                file.getFD().sync();
            } else if ("force".equals(m)) {
                file.getChannel().force(true);
            } else if ("forceFalse".equals(m)) {
                file.getChannel().force(false);
            } else {
                file.getFD().sync();
            }
        }

        @Override
        public FileChannel truncate(long newLength) throws IOException {
            // compatibility with JDK FileChannel#truncate
            if (readOnly) {
                throw new NonWritableChannelException();
            }
            if (newLength < file.length()) {
                file.setLength(newLength);
            }
            return this;
        }

        @Override
        public synchronized FileLock tryLock(long position, long size, boolean shared)
                throws IOException {
            return file.getChannel().tryLock(position, size, shared);
        }

        @Override
        public void implCloseChannel() throws IOException {
            file.close();
        }

        @Override
        public long position() throws IOException {
            return file.getFilePointer();
        }

        @Override
        public long size() throws IOException {
            return file.length();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            int len = file.read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
            if (len > 0) {
                dst.position(dst.position() + len);
            }
            return len;
        }

        @Override
        public FileChannel position(long pos) throws IOException {
            file.seek(pos);
            return this;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int len = src.remaining();
            file.write(src.array(), src.arrayOffset() + src.position(), len);
            src.position(src.position() + len);
            return len;
        }

        @Override
        public String toString() {
            return name;
        }

    }

}
