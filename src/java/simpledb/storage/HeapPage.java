package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import java.lang.Math;
import java.util.stream.Collectors;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    private boolean is_dirty_;
    private TransactionId dirty_tid_ = null;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to:
     * <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p>
     * where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        return (BufferPool.getPageSize() * 8) / (this.td.getSize() * 8 + 1);

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each
     * tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each
     *         tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {

        return (int) Math.ceil((double) this.numSlots / 8);

    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); // - numSlots *
                                                                                                 // td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should
     * be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("HeapPage::deleteTuple: tupleDesc is mismatch");
        }
        int potentinal_pos = 0;
        for (; potentinal_pos < numSlots; potentinal_pos++) {
            if (isSlotUsed(potentinal_pos)) {
                int tuple_size = td.numFields();
                int i = 0;
                for (; i < tuple_size; i++) {
                    if (!t.getField(i).equals(tuples[potentinal_pos].getField(i))) {
                        break;
                    }
                }
                if (i == tuple_size) {
                    break;
                }
            }
        }
        if (potentinal_pos == numSlots) {
            throw new DbException("deleteTuple: tuple is not on this page or already empty");
        }
        tuples[potentinal_pos] = null;
        markSlotUsed(potentinal_pos, false);
    }

    /**
     * Adds the specified tuple to the page; the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {

        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("tupleDesc is mismatch");
        }
        int potentinal_pos = 0;
        for (; potentinal_pos < numSlots; potentinal_pos++) {
            if (isSlotUsed(potentinal_pos)) {
                continue;
            } else {
                break;
            }
        }
        if (potentinal_pos == numSlots) {
            throw new DbException("page is full");
        }
        tuples[potentinal_pos] = t;
        RecordId rid = new RecordId(pid, potentinal_pos);
        t.setRecordId(rid);
        markSlotUsed(potentinal_pos, true);

    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        is_dirty_ = dirty;
        dirty_tid_ = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if
     * the page is not dirty
     */
    public TransactionId isDirty() {
        if (is_dirty_) {
            return dirty_tid_;
        } else {
            return null;
        }
    }

    /**
     * Returns the number of unused (i.e., empty) slots on this page.
     */
    public int getNumUnusedSlots() {
        int unused_slot_num = 0;
        for (byte header_byte : header) {
            int header_byte_int = header_byte & 0xff;
            while (header_byte_int != 0) {
                unused_slot_num += header_byte_int & 0x01;
                header_byte_int >>= 1;
                // System.out.println(header_byte_int);
            }
        }
        return numSlots - unused_slot_num;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int byte_num = i / 8;
        int byte_remain = i - 8 * byte_num;
        int header_byte_int = header[byte_num] & 0xff;
        header_byte_int >>= byte_remain;
        return (header_byte_int & 0x01) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {

        int byte_num = i / 8;
        int byte_remain = i - 8 * byte_num;
        int header_byte_int = value ? 1 : 0;
        header_byte_int <<= byte_remain;
        byte set_byte = (byte) (0xff - (1 << byte_remain));
        byte header_byte = (byte) header_byte_int;
        header[byte_num] = (byte) (header[byte_num] & set_byte);
        header[byte_num] = (byte) (header[byte_num] | header_byte);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this
     *         iterator throws an UnsupportedOperationException)
     *         (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {

        return Arrays.stream(tuples).filter(Objects::nonNull).collect(Collectors.toList()).iterator();
    }

}
