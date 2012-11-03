package simpledb.optimizing.statistics;

import java.util.NoSuchElementException;

import simpledb.Catalog;
import simpledb.Database;
import simpledb.TransactionId;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.file.DbFile;
import simpledb.file.DbFileIterator;
import simpledb.predicates.Predicate;
import simpledb.tuple.Field;
import simpledb.tuple.IntField;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;
import simpledb.tuple.Type;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
	private Histogram[] statFields;
	private long rowCount = 0;
	private int tableid;
	private int ioCpstPerPage;
	
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) 
    {
    	this.tableid = tableid;
    	this.ioCpstPerPage = ioCostPerPage;
    	Catalog catalog = Database.getCatalog();
		DbFile dbFile = catalog.getDbFile(tableid);
		TupleDesc tupleDesc = dbFile.getTupleDesc();
		int numFields = tupleDesc.numFields();
		MinMaxPair[] pairs = new MinMaxPair[numFields];
		statFields = new Histogram[numFields];
    	try 
    	{
    		getRowCount(dbFile);
    		for (int i = 0; i < numFields ; i++)
    		{
    			Type type = tupleDesc.getType(i);
    			switch(type)
    			{
					case INT_TYPE:
					{
						MinMaxPair computedMinMax = computeMinMaxForIntField(i, dbFile);
						pairs[i] = computedMinMax;
						break;
					}
					case STRING_TYPE:
					{
						pairs[i] = new MinMaxPair();
						break;
					}
    			}
    		}
    		for (int i = 0; i < numFields ; i++)
    		{
    			Histogram histogram = null;
    			Type type = tupleDesc.getType(i);
    			switch(type)
    			{
					case INT_TYPE:
					{
						histogram = new IntHistogram(NUM_HIST_BINS, pairs[i].min, pairs[i].max);
						break;
					}
					case STRING_TYPE:
					{
						histogram = new StringHistogram(NUM_HIST_BINS);
						break;
					}
    			}
    			createAndPopulateHistogram(i, dbFile,pairs[i], histogram);
    			statFields[i] = histogram;
    		}
		} 
    	catch (DbException e) 
    	{
		} 
    	catch (TransactionAbortedException e) 
    	{
		}
    }

    private void getRowCount(DbFile dbFile) throws NoSuchElementException, DbException, TransactionAbortedException 
    {
    	
    	DbFileIterator iterator = dbFile.iterator(new TransactionId());
    	try
    	{
    		iterator.open();
    		while(iterator.hasNext())
    		{
    			iterator.next();
    			rowCount++;
    		}
    	}
    	finally
    	{
    		iterator.close();
    	}
	}

	private void createAndPopulateHistogram(int i, DbFile dbFile, MinMaxPair minMaxPair, Histogram histogram) throws NoSuchElementException, DbException, TransactionAbortedException 
    {
    	TransactionId tid = new TransactionId();
    	DbFileIterator iter = dbFile.iterator(tid);
	    try
	    {
	    	iter.open();
    		while (iter.hasNext())
	    	{
	    		Tuple t = iter.next();
	    		Field field = t.getField(i);
	    		histogram.addValue(field);
	    	}
	    }
	    finally
	    {
	    	iter.close();
	    }
		
	}
	private MinMaxPair computeMinMaxForIntField(int field, DbFile dbFile) throws NoSuchElementException, DbException, TransactionAbortedException 
    {
    	Tuple t = null;
    	int first;
    	int second;
    	
    	MinMaxPair minMaxPair = new MinMaxPair();
    	TransactionId tid = new TransactionId();
    	DbFileIterator iter = dbFile.iterator(tid);
		try
		{
			iter.open();
			if (iter.hasNext())
			{
				t = iter.next();
				Field value1 = t.getField(field);
				first = ((IntField)value1).getValue();
				if (iter.hasNext())
				{
					t = iter.next();
					Field value2 = t.getField(field);
					second = ((IntField)value2).getValue();
				}
				else
				{
					minMaxPair.max = first;
					minMaxPair.min = first;
					return minMaxPair;
				}
				if (first > second)
				{
					minMaxPair.max = first;
					minMaxPair.min = second;
				}
				else
				{
					minMaxPair.max = second;
					minMaxPair.min = first;
				}
			}
			while(iter.hasNext())
			{
				t = iter.next();
				Field next = t.getField(field);
				int nextValue = ((IntField)next).getValue();
				if (nextValue > minMaxPair.max)
				{
					minMaxPair.max = nextValue;
				}
				else if (nextValue < minMaxPair.min)
				{
					minMaxPair.min = nextValue;
				}
			}
		}
		finally
		{
			iter.close();
		}
		return minMaxPair;
	}
	

	/** 
     * Estimates the cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() 
    {
    	Catalog catalog = Database.getCatalog();
    	DbFile dbFile = catalog.getDbFile(tableid);
		return dbFile.numPages()*ioCpstPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) 
    {
		return (int) (rowCount * selectivityFactor);
    }
    
    /**
     * 
     * @param field
     * @param selectivityFactor
     * @return the number of distinct values present in a column
     */
    public int estimateColumnCardinality(int field, double selectivityFactor)
    {
		return 0;
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) 
    {
        return statFields[field].estimateSelectivity(op, constant);
    }
    
    private class MinMaxPair
    {
    	int min;
    	int max;
    }

}
