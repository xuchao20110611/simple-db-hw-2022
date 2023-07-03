package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import java.lang.Math;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file_;
    private TupleDesc td_;
    private int page_num_;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        file_ = f;
        td_ = td;
        page_num_ = (int) Math.ceil(file_.length() / (double) BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file_;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file_.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td_;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId new_heappageid = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        // BufferPool.getPageSize()

        byte[] data = new byte[BufferPool.getPageSize()];
        HeapPage new_heappage;
        try {
            RandomAccessFile rf = new RandomAccessFile(file_, "r");
            rf.seek(BufferPool.getPageSize() * pid.getPageNumber());
            rf.read(data);
            rf.close();
            new_heappage = new HeapPage(new_heappageid, data);

        } catch (Exception e) {
            throw new IllegalArgumentException(" no such file");
        }

        return new_heappage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        int new_page_num = page.getId().getPageNumber();
        if (new_page_num < 0 || new_page_num > page_num_) {
            throw new IllegalArgumentException("HeapFile::writePage: no such page");
        }
        try {
            RandomAccessFile rf = new RandomAccessFile(file_, "rw");
            rf.seek(BufferPool.getPageSize() * new_page_num);
            rf.write(page.getPageData());
            rf.close();
            if (new_page_num == page_num_) {
                page_num_++;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("HeapFile::writePage: write failed");
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return page_num_;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> modified_page_list = new ArrayList<>();
        int pageid_pos = 0;
        for (; pageid_pos < page_num_; pageid_pos++) {
            PageId next_pageid = new HeapPageId(getId(), pageid_pos);
            Page next_page = Database.getBufferPool().getPage(tid, next_pageid, Permissions.READ_WRITE);
            try {
                synchronized (next_page) {
                    ((HeapPage) next_page).insertTuple(t);
                    modified_page_list.add(next_page);
                    return modified_page_list;
                }

            } catch (DbException e) {
                continue;
            }
        }
        if (pageid_pos == page_num_) {
            page_num_++;
            PageId next_pageid = new HeapPageId(getId(), pageid_pos);
            Page next_page = Database.getBufferPool().getPage(tid, next_pageid, Permissions.READ_WRITE);
            try {
                synchronized (next_page) {
                    ((HeapPage) next_page).insertTuple(t);
                    modified_page_list.add(next_page);
                    return modified_page_list;
                }

            } catch (DbException e) {

            }
            // throw new DbException("no empty page");
        }

        return null;// for compilation
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        List<Page> modified_page_list = new ArrayList<>();
        int pageid_pos = 0;
        for (; pageid_pos < page_num_; pageid_pos++) {
            PageId next_pageid = new HeapPageId(getId(), pageid_pos);
            Page next_page = Database.getBufferPool().getPage(tid, next_pageid, Permissions.READ_WRITE);
            try {
                synchronized (next_page) {
                    ((HeapPage) next_page).deleteTuple(t);
                    modified_page_list.add(next_page);
                    next_page.markDirty(true, tid);
                    return modified_page_list;
                }

            } catch (DbException e) {
                continue;
            }
        }
        if (pageid_pos == page_num_) {
            throw new DbException("no empty page");
        }

        return null;// for compilation
    }

    private class HeapFileIterator implements DbFileIterator {

        TransactionId tid_;
        HeapFile heapfile_;
        int pageid_pos_;
        Iterator<Tuple> tuple_ite_;

        public HeapFileIterator(TransactionId tid, HeapFile heapfile) {
            tid_ = tid;
            heapfile_ = heapfile;
            pageid_pos_ = 0;

        }

        @Override
        public void close() {
            tuple_ite_ = null;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tuple_ite_ == null) {
                return false;
            }
            if (tuple_ite_.hasNext()) {
                return true;
            }
            int pageid_pos = pageid_pos_;
            while (pageid_pos + 1 < page_num_) {
                pageid_pos += 1;
                PageId next_pageid = new HeapPageId(getId(), pageid_pos);
                Page next_page = Database.getBufferPool().getPage(tid_, next_pageid, Permissions.READ_ONLY);
                Iterator<Tuple> tuple_ite = ((HeapPage) next_page).iterator();
                if (tuple_ite.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("HeapFileIterator: next");
            }
            if (tuple_ite_.hasNext()) {
                return tuple_ite_.next();
            }
            while (pageid_pos_ + 1 < page_num_) {
                pageid_pos_ += 1;
                PageId next_pageid = new HeapPageId(getId(), pageid_pos_);
                Page next_page = Database.getBufferPool().getPage(tid_, next_pageid, Permissions.READ_ONLY);
                tuple_ite_ = ((HeapPage) next_page).iterator();
                if (tuple_ite_.hasNext()) {
                    return tuple_ite_.next();
                }
            }

            throw new NoSuchElementException("HeapFileIterator: next should not come here");
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageid_pos_ = 0;
            PageId next_pageid = new HeapPageId(getId(), pageid_pos_);
            Page next_page = Database.getBufferPool().getPage(tid_, next_pageid, Permissions.READ_ONLY);
            tuple_ite_ = ((HeapPage) next_page).iterator();
            // System.out.println("HeapFileIterator.open()");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageid_pos_ = 0;
            PageId next_pageid = new HeapPageId(getId(), pageid_pos_);
            Page next_page = Database.getBufferPool().getPage(tid_, next_pageid, Permissions.READ_ONLY);
            tuple_ite_ = ((HeapPage) next_page).iterator();
        }

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}
