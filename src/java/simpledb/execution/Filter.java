package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p_;
    private OpIterator child_;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        p_ = p;
        child_ = child;
    }

    public Predicate getPredicate() {
        return p_;
    }

    public TupleDesc getTupleDesc() {
        return child_.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child_.open();
    }

    public void close() {
        super.close();
        child_.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child_.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (child_.hasNext()) {
            Tuple tuple = child_.next();
            if (p_.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child_ };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child_ = children[0];
    }

}
