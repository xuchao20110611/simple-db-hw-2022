package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> items_ = new ArrayList<TDItem>();

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return items_.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        for (int i = 0; i < typeAr.length; i++) {
            TDItem new_item = new TDItem(typeAr[i], fieldAr[i]);
            items_.add(new_item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        for (int i = 0; i < typeAr.length; i++) {
            TDItem new_item = new TDItem(typeAr[i], typeAr[i].name());
            items_.add(new_item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items_.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= items_.size()) {
            throw new NoSuchElementException("getFieldName" + i);
        } else {
            return items_.get(i).fieldName;
        }

    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= items_.size()) {
            throw new NoSuchElementException("getFieldType: " + i);
        } else {
            return items_.get(i).fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        for (int i = 0; i < numFields(); i++) {
            if (items_.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("indexForFieldName" + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int total_size = 0;
        Iterator<TDItem> it = iterator();
        while (it.hasNext()) {
            TDItem item = it.next();
            total_size += item.fieldType.getLen();
        }
        return total_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] new_type = new Type[td1.numFields() + td2.numFields()];
        String[] new_field = new String[td1.numFields() + td2.numFields()];
        Iterator<TDItem> it1 = td1.iterator();
        Iterator<TDItem> it2 = td2.iterator();
        int i = 0;
        while (it1.hasNext()) {
            TDItem item = it1.next();
            new_type[i] = item.fieldType;
            new_field[i] = item.fieldName;
            i++;
        }
        while (it2.hasNext()) {
            TDItem item = it2.next();
            new_type[i] = item.fieldType;
            new_field[i] = item.fieldName;
            i++;
        }

        return new TupleDesc(new_type, new_field);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o.getClass() != TupleDesc.class) {
            return false;
        }
        if (((TupleDesc) o).numFields() != numFields()) {
            return false;
        }
        Iterator<TDItem> it1 = iterator();
        Iterator<TDItem> it2 = ((TupleDesc) o).iterator();
        while (it1.hasNext()) {
            TDItem item1 = it1.next();
            TDItem item2 = it2.next();
            if (!item1.fieldName.equals(item2.fieldName)) {
                return false;
            }
            if (!item1.fieldType.equals(item2.fieldType)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String ans = new String();
        int i = 0;
        for (; i + 1 < items_.size(); i++) {
            ans = ans + items_.get(i).toString() + ", ";
        }
        ans = ans + items_.get(i).toString();
        return ans;
    }
}
