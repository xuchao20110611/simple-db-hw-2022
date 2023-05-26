package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pid_;
    private int tuple_no_;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        pid_ = pid;
        tuple_no_ = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return tuple_no_;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pid_;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!((RecordId) o).pid_.equals(pid_) || ((RecordId) o).tuple_no_ != tuple_no_) {
            return false;
        }
        return true;

    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return 1031 * pid_.getPageNumber() + tuple_no_;
    }

}
