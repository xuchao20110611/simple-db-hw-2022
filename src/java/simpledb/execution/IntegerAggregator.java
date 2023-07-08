package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.NoSuchElementException;

import javax.swing.text.html.HTMLDocument.Iterator;

import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield_;
    private Type gbfieldtype_;
    private int afield_;
    private Op what_;
    private HashMap<String, ArrayList<Tuple>> int2tuples_ = new HashMap<String, ArrayList<Tuple>>(); // if no
                                                                                                     // grouping, all
                                                                                                     // with key 0

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
        if (int2tuples_.containsKey(key)) {
            int2tuples_.get(key).add(tup);
        } else {
            ArrayList<Tuple> tuples = new ArrayList<Tuple>();
            tuples.add(tup);
            int2tuples_.put(key, tuples);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        OpIterator return_ite = null;
        if (gbfield_ == NO_GROUPING) {
            return_ite = new IntegerNoGroupIterator();
        } else {
            return_ite = new IntegerGroupIterator();
        }
        return return_ite;
    }

    private class IntegerNoGroupIterator implements OpIterator {
        // This is for no grouping only
        private static final long serialVersionUID = 1L;
        private Tuple aggregate_tuple_; // no final to make it compile as consutructor has switch clause
        private final TupleDesc td_ = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggregateVal" });
        private boolean is_read_ = false;

        public IntegerNoGroupIterator() {
            switch (what_) {
                case MIN:
                    int min = Integer.MAX_VALUE;
                    for (String key : int2tuples_.keySet()) {
                        for (Tuple tuple : int2tuples_.get(key)) {
                            if (tuple.getField(afield_).hashCode() < min) {
                                min = tuple.getField(afield_).hashCode();
                            }
                        }
                    }
                    aggregate_tuple_ = new Tuple(td_);
                    aggregate_tuple_.setField(0, new IntField(min));
                    break;
                case MAX:
                    int max = Integer.MIN_VALUE;
                    for (String key : int2tuples_.keySet()) {
                        for (Tuple tuple : int2tuples_.get(key)) {
                            if (tuple.getField(afield_).hashCode() > max) {
                                max = tuple.getField(afield_).hashCode();
                            }
                        }
                    }
                    aggregate_tuple_ = new Tuple(td_);
                    aggregate_tuple_.setField(0, new IntField(max));
                    break;
                case SUM:
                    int sum = 0;
                    for (String key : int2tuples_.keySet()) {
                        for (Tuple tuple : int2tuples_.get(key)) {
                            sum += tuple.getField(afield_).hashCode();
                        }
                    }
                    aggregate_tuple_ = new Tuple(td_);
                    aggregate_tuple_.setField(0, new IntField(sum));
                    break;
                case AVG:
                    int sum2 = 0;
                    int count = 0;
                    for (String key : int2tuples_.keySet()) {
                        for (Tuple tuple : int2tuples_.get(key)) {
                            sum2 += tuple.getField(afield_).hashCode();
                            count++;
                        }
                    }
                    aggregate_tuple_ = new Tuple(td_);
                    aggregate_tuple_.setField(0, new IntField(sum2 / count));
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // attention: whether this is necessary?
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
            // attention: whether this is necessary?
            is_read_ = true;
        }
    }

    private class IntegerGroupIterator implements OpIterator {
        // This is for grouping only
        private static final long serialVersionUID = 1L;
        private ArrayList<Tuple> group_pairs_ = new ArrayList<Tuple>();
        private int current_index_ = 0;
        private final TupleDesc td_ = new TupleDesc(new Type[] { gbfieldtype_, Type.INT_TYPE },
                new String[] { "groupVal", "aggregateVal" });

        public IntegerGroupIterator() {
            switch (what_) {
                case MIN:
                    for (String key : int2tuples_.keySet()) {
                        int min = Integer.MAX_VALUE;
                        for (Tuple tuple : int2tuples_.get(key)) {
                            if (tuple.getField(afield_).hashCode() < min) {
                                min = tuple.getField(afield_).hashCode();
                            }
                        }
                        Tuple newtuple = new Tuple(td_);
                        if (gbfieldtype_ == Type.INT_TYPE) {
                            newtuple.setField(0, new IntField(Integer.parseInt(key)));
                        } else {
                            newtuple.setField(0, new StringField(key, key.length()));
                        }

                        newtuple.setField(1, new IntField(min));
                        group_pairs_.add(newtuple);
                    }
                    break;
                case MAX:
                    for (String key : int2tuples_.keySet()) {
                        int max = Integer.MIN_VALUE;
                        for (Tuple tuple : int2tuples_.get(key)) {
                            if (tuple.getField(afield_).hashCode() > max) {
                                max = tuple.getField(afield_).hashCode();
                            }
                        }
                        Tuple newtuple = new Tuple(td_);
                        if (gbfieldtype_ == Type.INT_TYPE) {
                            newtuple.setField(0, new IntField(Integer.parseInt(key)));
                        } else {
                            newtuple.setField(0, new StringField(key, key.length()));
                        }
                        newtuple.setField(1, new IntField(max));
                        group_pairs_.add(newtuple);
                    }
                    break;
                case SUM:
                    for (String key : int2tuples_.keySet()) {
                        int sum = 0;
                        for (Tuple tuple : int2tuples_.get(key)) {
                            sum += tuple.getField(afield_).hashCode();
                        }
                        Tuple newtuple = new Tuple(td_);
                        if (gbfieldtype_ == Type.INT_TYPE) {
                            newtuple.setField(0, new IntField(Integer.parseInt(key)));
                        } else {
                            newtuple.setField(0, new StringField(key, key.length()));
                        }
                        newtuple.setField(1, new IntField(sum));
                        group_pairs_.add(newtuple);
                    }
                    break;
                case AVG:
                    for (String key : int2tuples_.keySet()) {
                        int sum = 0;
                        for (Tuple tuple : int2tuples_.get(key)) {
                            sum += tuple.getField(afield_).hashCode();
                        }
                        Tuple newtuple = new Tuple(td_);
                        if (gbfieldtype_ == Type.INT_TYPE) {
                            newtuple.setField(0, new IntField(Integer.parseInt(key)));
                        } else {
                            newtuple.setField(0, new StringField(key, key.length()));
                        }
                        newtuple.setField(1, new IntField(sum / int2tuples_.get(key).size()));
                        group_pairs_.add(newtuple);
                    }
                    break;
                case COUNT:
                    for (String key : int2tuples_.keySet()) {
                        Tuple newtuple = new Tuple(td_);
                        if (gbfieldtype_ == Type.INT_TYPE) {
                            newtuple.setField(0, new IntField(Integer.parseInt(key)));
                        } else {
                            newtuple.setField(0, new StringField(key, key.length()));
                        }
                        newtuple.setField(1, new IntField(int2tuples_.get(key).size()));
                        group_pairs_.add(newtuple);
                    }
                    break;
                default:
                    System.out.println("Invalid operator");
            }

        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // attention: whether it is necessary to reset the current_index_ to 0
            current_index_ = 0;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return current_index_ < group_pairs_.size();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return group_pairs_.get(current_index_++);
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
            // attention: whether it is necessary to reset the current_index_ to 0
            current_index_ = 0;
        }
    }

}
