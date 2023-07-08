package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t_;
    private OpIterator child_;
    private boolean called_ = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        t_ = t;
        child_ = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called_) {
            return null;
        }
        called_ = true;
        int count = 0;
        while (child_.hasNext()) {
            Tuple tuple = child_.next();
            try {
                Database.getBufferPool().deleteTuple(t_, tuple);
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
        Tuple result = new Tuple(td);
        result.setField(0, new IntField(count));
        return result;
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
