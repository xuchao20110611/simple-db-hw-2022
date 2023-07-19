package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.HeapPage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int num_pages_ = 0;

    private ConcurrentHashMap<PageId, Page> pid_to_pages_ = new ConcurrentHashMap<PageId, Page>();

    private ConcurrentHashMap<TransactionId, Set<PageId>> tid_to_pids_rw_ = new ConcurrentHashMap<TransactionId, Set<PageId>>();
    private ConcurrentHashMap<TransactionId, Set<PageId>> tid_to_pids_ro_ = new ConcurrentHashMap<TransactionId, Set<PageId>>();
    private ConcurrentHashMap<PageId, C_Lock> pid_to_lock_ = new ConcurrentHashMap<PageId, C_Lock>();

    private class C_Lock {
        private Set<TransactionId> read_lock_ = new HashSet<TransactionId>();
        private TransactionId write_lock_ = null;

        public C_Lock() {
        }

        public Set<TransactionId> getReadLock() {
            return read_lock_;
        }

        public void addReadLock(TransactionId tid) {
            read_lock_.add(tid);
        }

        public void removeReadLock(TransactionId tid) {
            read_lock_.remove(tid);
        }

        public TransactionId getWriteLock() {
            return write_lock_;
        }

        public void removeWriteLock() {
            write_lock_ = null;
        }

        public void setWriteLock(TransactionId tid) {
            write_lock_ = tid;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        num_pages_ = numPages;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it
     * is present, it should be returned. If it is not present, it should
     * be added to the buffer pool and returned. If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        // tid_to_permission_.put(tid, perm);
        synchronized (pid_to_lock_) {
            if (pid_to_lock_.get(pid) == null) {
                pid_to_lock_.put(pid, new C_Lock());
            }
        }
        C_Lock lock = pid_to_lock_.get(pid);
        synchronized (lock) {
            if (perm.equals(Permissions.READ_ONLY)) {
                if (tid_to_pids_rw_.get(tid) != null && tid_to_pids_rw_.get(tid).contains(pid)) {
                    tid_to_pids_rw_.get(tid).remove(pid);
                    lock.removeWriteLock();
                }
                if (lock.getWriteLock() != null && !lock.getWriteLock().equals(tid)) {
                    throw new DbException("BufferPool::getPage: can not add a radlock when there is a write lock");
                }
                lock.addReadLock(tid);

                tid_to_pids_ro_.putIfAbsent(tid, new HashSet<PageId>());
                tid_to_pids_ro_.get(tid).add(pid);

            } else {
                if (tid_to_pids_ro_.get(tid) != null && tid_to_pids_ro_.get(tid).contains(pid)) {
                    tid_to_pids_ro_.get(tid).remove(pid);
                    lock.removeReadLock(tid);
                }
                if (lock.getWriteLock() != null && !lock.getWriteLock().equals(tid)) {
                    throw new DbException("BufferPool::getPage: can not add a write lock when there is a write lock");
                }
                if (lock.getReadLock().size() > 0) {
                    throw new DbException("BufferPool::getPage: can not add a write lock when there are read locks");
                }
                lock.setWriteLock(tid);

                tid_to_pids_rw_.putIfAbsent(tid, new HashSet<PageId>());
                tid_to_pids_rw_.get(tid).add(pid);

            }
        }

        if (pid_to_pages_.size() == num_pages_ && pid_to_pages_.get(pid) == null) {
            evictPage();
        }

        // tid_to_pids_.putIfAbsent(tid, new HashSet<PageId>());
        // tid_to_pids_.get(tid).add(pid);
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (pid_to_pages_.get(pid) == null) {
            pid_to_pages_.put(pid, dbfile.readPage(pid));
        }

        return pid_to_pages_.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {

        C_Lock lock = pid_to_lock_.get(pid);
        synchronized (lock) {
            if (lock.getReadLock().contains(tid)) {
                lock.removeReadLock(tid);
                tid_to_pids_ro_.get(tid).remove(pid);
            }
            if (lock.getWriteLock() != null && lock.getWriteLock().equals(tid)) {
                lock.removeWriteLock();
                tid_to_pids_rw_.get(tid).remove(pid);
            }
        }
        // if (tid_to_pids_ro_.get(tid) != null &&
        // tid_to_pids_ro_.get(tid).contains(pid)) {
        // tid_to_pids_ro_.get(tid).remove(pid);
        // pid_to_lock_.get(pid).readLock().unlock();
        // }

        // if (tid_to_pids_rw_.get(tid) != null &&
        // tid_to_pids_rw_.get(tid).contains(pid)) {
        // tid_to_pids_rw_.get(tid).remove(pid);
        // pid_to_lock_.get(pid).writeLock().unlock();
        // }

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {

        if (tid_to_pids_ro_.get(tid) != null && tid_to_pids_ro_.get(tid).contains(p)) {
            return true;
        }

        if (tid_to_pids_rw_.get(tid) != null && tid_to_pids_rw_.get(tid).contains(p)) {
            return true;
        }

        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        // int page_num = 0;
        // boolean is_insert = false;

        List<Page> modified_page = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : modified_page) {
            page.markDirty(true, tid);
            pid_to_pages_.put(page.getId(), page);
            // tid_to_pids_rw_.putIfAbsent(tid, new HashSet<PageId>());
            // tid_to_pids_rw_.get(tid).add(page.getId());
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // int page_num = 0;
        // boolean is_delete = false;
        int tableId = t.getRecordId().getPageId().getTableId();
        List<Page> modified_page = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for (Page page : modified_page) {
            page.markDirty(true, tid);
            pid_to_pages_.put(page.getId(), page);
            // tid_to_pids_rw_.putIfAbsent(tid, new HashSet<PageId>());
            // tid_to_pids_rw_.get(tid).add(page.getId());
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pid_to_pages_.keySet()) {

            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        pid_to_pages_.remove(pid);
        // pid_to_tid_.remove(pid.hashCode()); // question
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {

        Page page = pid_to_pages_.get(pid);
        if (page == null)
            return;
        if (page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }

    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : tid_to_pids_rw_.get(tid)) {
            flushPage(pid);
        }
        for (PageId pid : tid_to_pids_ro_.get(tid)) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // discard one page randomly
        PageId[] keys = pid_to_pages_.keySet().toArray(new PageId[0]);
        Random random = new Random();

        while (true) {
            PageId random_key = keys[random.nextInt(keys.length)];
            try {
                flushPage(random_key);
                pid_to_pages_.remove(random_key);
                // pid_to_tid_.remove(random_key);
                break;
            } catch (IOException e) {

            }
        }

    }

}
