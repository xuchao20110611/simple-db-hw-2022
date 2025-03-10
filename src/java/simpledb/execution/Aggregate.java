package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private Aggregator aggregator_;
    private OpIterator child_;
    private int afield_;
    private int gfield_;
    private Aggregator.Op aop_;
    OpIterator it_;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        Type aggregator_type = null;
        if (gfield != -1) {
            aggregator_type = child.getTupleDesc().getFieldType(gfield);
        }
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {

            aggregator_ = new IntegerAggregator(gfield, aggregator_type, afield, aop);
        } else {
            aggregator_ = new StringAggregator(gfield, aggregator_type, afield, aop);
        }
        child_ = child;
        afield_ = afield;
        gfield_ = gfield;
        aop_ = aop;

        try {
            child_.open();
            while (child_.hasNext()) {
                aggregator_.mergeTupleIntoGroup(child_.next());
            }
        } catch (Exception e) {

        }
        it_ = aggregator_.iterator();

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (gfield_ != -1) {
            return gfield_;
        }
        return Aggregator.NO_GROUPING;

    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        if (groupField() == Aggregator.NO_GROUPING) {
            return null;
        }
        return child_.getTupleDesc().getFieldName(groupField());
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield_;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        return child_.getTupleDesc().getFieldName(afield_);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop_;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        it_.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple next_tuple = null;
        try {
            if (it_.hasNext()) {

                next_tuple = it_.next();
            }
        } catch (NoSuchElementException e) {
        }
        return next_tuple;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it_.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String agg_col = child_.getTupleDesc().getFieldName(afield_);
        TupleDesc td = null;
        if (gfield_ == -1) {
            td = new TupleDesc(new Type[] { child_.getTupleDesc().getFieldType(afield_) }, new String[] { agg_col });
        } else {
            td = new TupleDesc(
                    new Type[] { child_.getTupleDesc().getFieldType(gfield_),
                            child_.getTupleDesc().getFieldType(afield_) },
                    new String[] { child_.getTupleDesc().getFieldName(gfield_), agg_col });
        }
        return td;
    }

    public void close() {
        it_.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {

        return new OpIterator[] { child_ };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child_ = children[0];
        if (child_.getTupleDesc().getFieldType(afield_) == Type.INT_TYPE) {
            aggregator_ = new IntegerAggregator(gfield_, child_.getTupleDesc().getFieldType(gfield_), afield_, aop_);
        } else {
            aggregator_ = new StringAggregator(gfield_, child_.getTupleDesc().getFieldType(gfield_), afield_, aop_);
        }
        try {
            while (child_.hasNext()) {
                aggregator_.mergeTupleIntoGroup(child_.next());
            }
        } catch (Exception e) {

        }
        it_ = aggregator_.iterator();
    }

}
