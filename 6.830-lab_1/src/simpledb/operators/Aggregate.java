package simpledb.operators;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import simpledb.aggregates.Aggregator;
import simpledb.aggregates.IntAggregator;
import simpledb.aggregates.StringAggregator;
import simpledb.aggregates.Aggregator.Op;
import simpledb.exceptions.DbException;
import simpledb.exceptions.TransactionAbortedException;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;
import simpledb.tuple.Type;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

	
	private DbIterator aggItr;
	private DbIterator child;
	private int afield;
	private int gfield;
	private Aggregator.Op aop;
	
    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) 
    {
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    }

    private DbIterator buildAggregator(DbIterator child, int afield, int gfield,Op op) 
    {
    	DbIterator aitr = new TupleArrayIterator(new ArrayList<Tuple>());
    	Type groupType;
    	if (gfield == Aggregator.NO_GROUPING)
    	{
    		groupType = Type.INT_TYPE;
    	}
    	else
    	{
    		groupType = child.getTupleDesc().getType(gfield);
    	}
    	try 
    	{
			child.open();
    		if(groupType == Type.INT_TYPE)
			{
				IntAggregator intAgg = new IntAggregator(gfield, Type.INT_TYPE, afield, op);
				
					for(;child.hasNext();)
					{
						Tuple tup = child.next();
						intAgg.merge(tup);
					}
				aitr = intAgg.iterator();	
			} 
			
			else if(groupType == Type.STRING_TYPE)
			{
				StringAggregator stringAgg = new StringAggregator(gfield, Type.STRING_TYPE, afield, op);
				
				for(;child.hasNext();)
				{
					Tuple tup = child.next();
					stringAgg.merge(tup);
				}
			aitr = stringAgg.iterator();	
			}
		}
		
		catch (NoSuchElementException e) 
		{
			System.out.println(e);
		} 
		catch (DbException e) 
		{
			System.out.println(e);
		} 
		catch (TransactionAbortedException e) 
		{
			System.out.println(e);
		}
    	finally
    	{
    		child.close();
    	}
		return aitr;
		
	}

	public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException 
    {
        aggItr = buildAggregator(child, afield, gfield, aop);
    	aggItr.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
    	Tuple t = null;
    	if (!aggItr.hasNext())
    	{
    		return t;
    	}
    	return aggItr.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggItr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() 
    {
    	TupleDesc res = null;
    	TupleDesc tupleDesc = child.getTupleDesc();
    	Type aggType = tupleDesc.getType(afield);
    	String aggFieldName = tupleDesc.getFieldName(afield);
    	if (gfield == Aggregator.NO_GROUPING)
    	{
    		res = new TupleDesc(new Type[]{aggType}, new String[]{aggFieldName});
    	}
    	else
    	{
    		Type groupType = tupleDesc.getType(gfield);
        	String groupFieldName = tupleDesc.getFieldName(gfield);
        	res = new TupleDesc(new Type[]{aggType, groupType}, new String[]{aggFieldName, groupFieldName});
    	}
    	return res;
    }

    public void close() {
        aggItr.close();
    }
}
