package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield_;
    private Type gbfieldtype_;
    private int afield_;
    private Op what_;
    private HashMap<String, ArrayList<Tuple>> str2tuples_ = new HashMap<String, ArrayList<Tuple>>();// if no grouping,
                                                                                                    // all with the
                                                                                                    // key
                                                                                                    // ""

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbfield_ = gbfield;
        gbfieldtype_ = gbfieldtype;
        afield_ = afield;
        what_ = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        String key = null;
        if (gbfield_ == NO_GROUPING) {
            key = "";
        } else {
            key = tup.getField(gbfield_).toString();
        }
        if (str2tuples_.containsKey(key)) {
            str2tuples_.get(key).add(tup);
        } else {
            ArrayList<Tuple> tuples = new ArrayList<Tuple>();
            tuples.add(tup);
            str2tuples_.put(key, tuples);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        OpIterator opIterator = null;
        if (gbfield_ == NO_GROUPING) {
            opIterator = new StringNoGroupIterator();
        } else {
            opIterator = new StringGroupIterator();
        }
        return opIterator;
    }

    private class StringNoGroupIterator implements OpIterator {
        // This is for no grouping only
        private static final long serialVersionUID = 1L;
        private Tuple aggregate_tuple_; // no final to make it compile as consutructor has switch clause
        private final TupleDesc td_ = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggregateVal" });
        private boolean is_read_ = false;

        public StringNoGroupIterator() {
            switch (what_) {
                case COUNT:
                    aggregate_tuple_ = new Tuple(td_);
                    aggregate_tuple_.setField(0, new IntField(str2tuples_.get("").size()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operation " + what_);
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // attention: whether this is needed?
            is_read_ = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return !is_read_;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                is_read_ = true;
                return aggregate_tuple_;
            }
            throw new NoSuchElementException("No more tuples");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            is_read_ = false;
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td_;
        }

        @Override
        public void close() {
            // attention: whether this is needed?
            is_read_ = false;
        }

    }

    private class StringGroupIterator implements OpIterator {

        // This is for grouping only
        private static final long serialVersionUID = 1L;
        private ArrayList<Tuple> group_pairs_ = new ArrayList<Tuple>();
        private int current_index_ = 0;
        // only supports COUNT
        private final TupleDesc td_ = new TupleDesc(new Type[] { gbfieldtype_, Type.INT_TYPE },
                new String[] { "groupVal", "aggregateVal" });

        public StringGroupIterator() {
            // only implements support for COUNT
            for (String key : str2tuples_.keySet()) {
                Tuple tuple = new Tuple(td_);
                Field field = null;
                if (gbfieldtype_ == Type.INT_TYPE) {
                    field = new IntField(Integer.valueOf(key));
                } else {
                    field = new StringField(key, key.length());
                }
                tuple.setField(0, field);
                tuple.setField(1, new IntField(str2tuples_.get(key).size()));
                group_pairs_.add(tuple);
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // attention: whether this is needed?
            current_index_ = 0;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return current_index_ < group_pairs_.size();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return group_pairs_.get(current_index_++);
            }
            throw new NoSuchElementException("No more tuples");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            current_index_ = 0;
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td_;
        }

        @Override
        public void close() {
            // attention: whether this is needed?
            current_index_ = 0;
        }

    }
}
