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

    private ConcurrentHashMap<Integer, Page> pid_to_pages_ = new ConcurrentHashMap<Integer, Page>();
    private ConcurrentHashMap<Integer, TransactionId> pid_to_tid_ = new ConcurrentHashMap<Integer, TransactionId>();
    private ConcurrentHashMap<TransactionId, Permissions> tid_to_permission_ = new ConcurrentHashMap<TransactionId, Permissions>();

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
        tid_to_permission_.put(tid, perm);
        if (pid_to_pages_.size() == num_pages_ && pid_to_pages_.get(pid.getPageNumber()) == null) {
            // unimplemented: eviction rules
            // throw new DbException("BufferPool::getPage: full bufferpage");
        }
        int table_page_id = pid.getTableId() * 1025 + pid.getPageNumber();
        if (pid_to_tid_.containsKey(table_page_id) && pid_to_tid_.get(table_page_id) != tid) {
            /*
             * unimplemented: when evicted we need to clear the according recording in the
             * map
             */
            // throw new TransactionAbortedException();
        }

        pid_to_tid_.put(table_page_id, tid);
        DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        if (pid_to_pages_.get(table_page_id) == null) {
            pid_to_pages_.put(table_page_id, dbfile.readPage(pid));
        }

        return pid_to_pages_.get(table_page_id);
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
        // TODO: some code goes here
        // not necessary for lab1|lab2
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
        // TODO: some code goes here
        // not necessary for lab1|lab2
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

        int page_num = 0;
        boolean is_insert = false;
        tid_to_permission_.put(tid, Permissions.READ_WRITE);
        while (!is_insert) {
            PageId pid = new HeapPageId(tableId, page_num);
            Page page = getPage(tid, pid, Permissions.READ_WRITE);

            page.markDirty(true, tid);
            try {
                synchronized (page) {
                    ((HeapPage) page).insertTuple(t);
                    is_insert = true;
                }

            } catch (DbException e) {
                page_num++;
            }

        }
        if (!is_insert) {
            // make a new page
            HeapPageId pid = new HeapPageId(tableId, page_num);
            HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
            int table_page_id = tableId * 1025 + page_num;
            // unimplemented: eviction rules
            pid_to_pages_.put(table_page_id, page);
            pid_to_tid_.put(table_page_id, tid);
            page.insertTuple(t);
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
        int page_num = 0;
        boolean is_delete = false;
        tid_to_permission_.put(tid, Permissions.READ_WRITE);
        while (!is_delete) {
            PageId pid = new HeapPageId(t.getRecordId().getPageId().getTableId(), page_num);
            Page page = getPage(tid, pid, Permissions.READ_WRITE);

            page.markDirty(true, tid);
            try {
                synchronized (page) {
                    ((HeapPage) page).deleteTuple(t);
                    is_delete = true;
                }

            } catch (DbException e) {
                page_num++;
            }
        }
        if (!is_delete) {
            throw new DbException("BufferPool::deleteTuple: delete failed");
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1

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
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
    }

}
