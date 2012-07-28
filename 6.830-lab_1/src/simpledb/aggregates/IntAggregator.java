package simpledb.aggregates;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import simpledb.Utility;
import simpledb.aggregates.Aggregator.Op;
import simpledb.operators.Aggregate;
import simpledb.operators.DbIterator;
import simpledb.operators.TupleArrayIterator;
import simpledb.tuple.Field;
import simpledb.tuple.IntField;
import simpledb.tuple.Tuple;
import simpledb.tuple.Type;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator extends Aggregator {
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) 
    {
    	super(gbfield, gbfieldtype, afield, what);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) 
    {
    	super.merge(tup);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() 
    {
    	return super.iterator();
    }
}
