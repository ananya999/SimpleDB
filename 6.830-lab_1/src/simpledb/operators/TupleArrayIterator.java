package simpledb.operators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

public class TupleArrayIterator implements DbIterator {
    ArrayList<Tuple> tups;
    Iterator<Tuple> it = null;

    public TupleArrayIterator(ArrayList<Tuple> tups) {
        this.tups = tups;
    }

    public void open()
        throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /** @return true if the iterator has more items. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator, or null if there are no more tuples.

     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return it.next();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /**
     * Returns the TupleDesc associated with this DbIterator.
     */
    public TupleDesc getTupleDesc() {
        return tups.get(0).getTupleDesc();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    }

}
