package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid_;
    private int ioCostPerPage_;
    private int tuple_num_;
    HeapFile file_;
    ArrayList<Object> histograms_;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate
     *                      between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        tableid_ = tableid;
        file_ = (HeapFile) Database.getCatalog().getDatabaseFile(tableid_);
        ioCostPerPage_ = ioCostPerPage;

        TransactionId initiate_tid = new TransactionId();
        DbFileIterator ite = file_.iterator(initiate_tid);

        try {
            ite.open();
            int[] maxnums = null;
            int[] minnums = null;
            int num_fields = 0;
            Tuple first_tuple = null;
            ArrayList<Object> values = new ArrayList<>();
            if (ite.hasNext()) {
                first_tuple = ite.next();
                num_fields = first_tuple.getTupleDesc().numFields();
                histograms_ = new ArrayList<>(num_fields);
                maxnums = new int[num_fields];
                minnums = new int[num_fields];
                tuple_num_++;
                for (int i = 0; i < num_fields; i++) {
                    if (first_tuple.getField(i).getType() == Type.INT_TYPE) {
                        maxnums[i] = ((IntField) first_tuple.getField(i)).getValue();
                        minnums[i] = ((IntField) first_tuple.getField(i)).getValue();
                        ArrayList<Integer> value_ele = new ArrayList<>();
                        value_ele.add(((IntField) first_tuple.getField(i)).getValue());
                        values.add(value_ele);
                    } else {
                        ArrayList<String> value_ele = new ArrayList<>();
                        value_ele.add(((StringField) first_tuple.getField(i)).getValue());
                        values.add(value_ele);
                    }
                }
            }
            while (ite.hasNext()) {
                Tuple tuple = ite.next();
                tuple_num_++;
                for (int i = 0; i < num_fields; i++) {
                    if (tuple.getField(i).getType() == Type.INT_TYPE) {
                        maxnums[i] = Math.max(maxnums[i], ((IntField) tuple.getField(i)).getValue());
                        minnums[i] = Math.min(minnums[i], ((IntField) tuple.getField(i)).getValue());
                        ((ArrayList<Integer>) values.get(i)).add(((IntField) tuple.getField(i)).getValue());
                    } else {
                        ((ArrayList<String>) values.get(i)).add(((StringField) tuple.getField(i)).getValue());
                    }
                }
            }
            for (int i = 0; i < num_fields; i++) {
                if (first_tuple.getField(i).getType() == Type.INT_TYPE) {
                    IntHistogram new_hist = new IntHistogram(NUM_HIST_BINS, minnums[i], maxnums[i]);
                    for (int j = 0; j < ((ArrayList<Integer>) values.get(i)).size(); j++) {
                        new_hist.addValue(((ArrayList<Integer>) values.get(i)).get(j));
                    }
                    histograms_.add(new_hist);
                } else {
                    StringHistogram new_hist = new StringHistogram(NUM_HIST_BINS);
                    for (int j = 0; j < ((ArrayList<String>) values.get(i)).size(); j++) {
                        new_hist.addValue(((ArrayList<String>) values.get(i)).get(j));
                    }
                    histograms_.add(new_hist);
                }
            }
            ite.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return file_.numPages() * ioCostPerPage_;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // TODO: some code goes here
        return 0;
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then
     *              given a
     *              tuple, of which we do not know the value of the field, return
     *              the
     *              expected selectivity. You may estimate this value from the
     *              histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (histograms_.get(field) instanceof IntHistogram) {
            IntHistogram intHistogram = (IntHistogram) histograms_.get(field);
            return intHistogram.avgSelectivity();
        } else {
            StringHistogram stringHistogram = (StringHistogram) histograms_.get(field);
            return stringHistogram.avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        switch (constant.getType()) {
            case INT_TYPE:
                IntHistogram intHistogram = (IntHistogram) histograms_.get(field);
                return intHistogram.estimateSelectivity(op, ((IntField) constant).getValue());
            case STRING_TYPE:
                StringHistogram stringHistogram = (StringHistogram) histograms_.get(field);
                return stringHistogram.estimateSelectivity(op, ((StringField) constant).getValue());
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return tuple_num_;
    }

}
