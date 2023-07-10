package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

    private int min_;
    private int max_;
    private int width_;
    private int ntups_ = 0;
    private int[] buckets_count_; // the last one has a different width

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it
     * receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through
     * the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed. For
     * example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this
     *                class for histogramming
     * @param max     The maximum integer value that will ever be passed to this
     *                class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {

        min_ = min;
        max_ = max;
        width_ = (max - min + 1) / buckets;
        width_ = Math.max(width_, 1);
        buckets_count_ = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = (v - min_) / width_;
        if (index >= buckets_count_.length)
            index = buckets_count_.length - 1;
        buckets_count_[index]++; // v should be in the range of [min_, max_]
        ntups_++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v > max_) {
            switch (op) {
                case EQUALS:
                    return 0;
                case GREATER_THAN:
                    return 0;
                case GREATER_THAN_OR_EQ:
                    return 0;
                case LESS_THAN:
                    return 1;
                case LESS_THAN_OR_EQ:
                    return 1;
                case LIKE:
                    return 0;
                case NOT_EQUALS:
                    return 1.0;
                default:
                    return -1.0;
            }
        }
        if (v < min_) {
            switch (op) {
                case EQUALS:
                    return 0;
                case GREATER_THAN:
                    return 1.0;
                case GREATER_THAN_OR_EQ:
                    return 1.0;
                case LESS_THAN:
                    return 0;
                case LESS_THAN_OR_EQ:
                    return 0;
                case LIKE:
                    return 0;
                case NOT_EQUALS:
                    return 1.0;
                default:
                    return -1.0;
            }
        }
        int index = (v - min_) / width_;
        if (index >= buckets_count_.length)
            index = buckets_count_.length - 1;
        double equal_selectivity = 0;
        double greater_than_selectivity = 0;
        double less_than_selectivity = 0;
        int b_left = min_ + index * width_;
        int b_right = b_left + width_ - 1;
        if (index == buckets_count_.length - 1) {
            b_right = max_;
        }
        if (index == buckets_count_.length - 1) {
            equal_selectivity = buckets_count_[index] / (double) (width_ + (max_ - min_ + 1) % width_);
            greater_than_selectivity = buckets_count_[index] * (b_right - v)
                    / (double) (width_ + (max_ - min_ + 1) % width_);
            less_than_selectivity = buckets_count_[index] * (v - b_left)
                    / (double) (width_ + (max_ - min_ + 1) % width_);
        } else {
            equal_selectivity = buckets_count_[index] / (double) width_;
            greater_than_selectivity = buckets_count_[index] * (b_right - v) / (double) width_;
            less_than_selectivity = buckets_count_[index] * (v - b_left) / (double) width_;
        }
        for (int i = 0; i < index; i++) {
            less_than_selectivity += buckets_count_[i];
        }
        for (int i = index + 1; i < buckets_count_.length; i++) {
            greater_than_selectivity += buckets_count_[i];
        }
        // less_than_selectivity = less_than_selectivity / ntups_;
        // greater_than_selectivity = greater_than_selectivity / ntups_;
        // equal_selectivity = equal_selectivity / ntups_;

        switch (op) {
            case EQUALS:
                return equal_selectivity / ntups_;
            case GREATER_THAN:
                return greater_than_selectivity / ntups_;
            case GREATER_THAN_OR_EQ:
                return (greater_than_selectivity + equal_selectivity) / ntups_;
            case LESS_THAN:
                return less_than_selectivity / ntups_;
            case LESS_THAN_OR_EQ:
                return (less_than_selectivity + equal_selectivity) / ntups_;
            case LIKE:
                return 1.0;
            case NOT_EQUALS:
                return 1.0 - equal_selectivity / ntups_;
            default:
                break;
        }

        return -1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return null;
    }
}
