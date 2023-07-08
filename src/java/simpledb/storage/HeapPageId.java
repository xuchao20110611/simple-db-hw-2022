package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

    private int table_id_;
    private int page_num_;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        table_id_ = tableId;
        page_num_ = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        return table_id_;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int getPageNumber() {
        return page_num_;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        return table_id_ * 1025 + page_num_;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        if (!(o instanceof HeapPageId))
            return false;
        if (((HeapPageId) o).page_num_ == page_num_
                && ((HeapPageId) o).table_id_ == table_id_) {
            return true;
        }
        return false;

    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk. Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
