package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import simpledb.common.Type;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t_;
    private OpIterator child_;
    private int tableId_;
    private boolean called_ = false;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we
     *                     are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        t_ = t;
        child_ = child;
        tableId_ = tableId;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] { Type.INT_TYPE });
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child_.open();
    }

    public void close() {
        child_.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child_.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!called_) {
            called_ = true;
            int count = 0;
            while (child_.hasNext()) {
                Tuple t = child_.next();

                try {
                    Database.getBufferPool().insertTuple(t_, tableId_, t);
                    count++;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
            Tuple result = new Tuple(td);
            result.setField(0, new IntField(count));
            return result;
        } else {
            return null;
        }

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
