package simpledb.operators;
import java.util.NoSuchElementException;

import simpledb.Catalog;
import simpledb.Database;
import simpledb.TransactionId;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.file.DbFile;
import simpledb.file.DbFileIterator;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

	Catalog catalog = Database.getCatalog();
	private DbFileIterator fileIter;
	private TupleDesc td;
	
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) 
    {
    	DbFile dbFile = catalog.getDbFile(tableid);
    	td = new TupleDesc(dbFile.getTupleDesc());
    	td.setAliasToFields(tableAlias);
    	fileIter = dbFile.iterator(tid);
    }

    public void open() throws DbException, TransactionAbortedException 
    {
    	fileIter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
    	return fileIter.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
    	return fileIter.next();
    }

    public void close() {
        fileIter.close();
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        fileIter.rewind();
    }
}
