package simpledb.aggregates;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import simpledb.Utility;
import simpledb.operators.Aggregate;
import simpledb.operators.DbIterator;
import simpledb.operators.TupleArrayIterator;
import simpledb.tuple.Field;
import simpledb.tuple.IntField;
import simpledb.tuple.StringField;
import simpledb.tuple.Tuple;
import simpledb.tuple.Type;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public class Aggregator {
    public static final int NO_GROUPING = -1;

    public enum Op {
        MIN, MAX, SUM, AVG, COUNT;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
    }
    
    private Op operator;
	private int gbfield;
	private int afield;
	private Type gbfieldtype;
	private HashMap<Field, IntField> aggMap;
	private ArrayList<Map.Entry<Field, Field>> fullList;

	
	public Aggregator(int gbfield, Type gbfieldtype, int afield, Op what) 
    {
    	operator = what;
    	this.gbfield = gbfield;
    	this.afield = afield;
    	fullList = new ArrayList<Entry<Field,Field>>();
    	this.gbfieldtype = gbfieldtype;
    }
	
    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup)
    {
    	if (gbfield == Aggregator.NO_GROUPING)
    	{
    		fullList.add(new AbstractMap.SimpleEntry<Field, Field>(new IntField(-1), tup.getField(afield)));
    	}
    	else
    	{
    		fullList.add(new AbstractMap.SimpleEntry<Field, Field>(tup.getField(gbfield), tup.getField(afield)));
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * @see simpledb.operators.TupleIterator for a possible helper
     */
    public DbIterator iterator(){
    	buildAggregatesMap();
    	
    	Tuple tuple;
    	Set<Entry<Field, IntField>> entrySet = aggMap.entrySet();
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	
    	if (aggMap.containsKey(new IntField(Aggregator.NO_GROUPING)))
		{
			tuple = Utility.getTuple(new int[]{aggMap.get(new IntField(Aggregator.NO_GROUPING)).getValue()}, 1);
		}
    	else
    	{
	    	for (Entry<Field, IntField> entry : entrySet) 
	    	{
	    		Field key = entry.getKey();
	    		IntField value = entry.getValue();
	    		int agg = value.getValue();
	    		if (gbfieldtype == Type.INT_TYPE)
	    		{
	    			int groupByInt = ((IntField)key).getValue();
	    			tuple = Utility.getTuple(new int[]{groupByInt, agg}, 2);
	    		}
	    		else
	    		{
	    			String groupByString = ((StringField)key).getValue();
	    			tuple = Utility.getTuple(new Field[]{
	    					new StringField(groupByString, groupByString.length()), new IntField(agg)}, 2);
	    		}
	    		tuples.add(tuple);
			}
    	}
    	return new TupleArrayIterator(tuples);	
    	
    }
    
    private void buildAggregatesMap()
    {
    	try
    	{
    		aggMap = new HashMap<Field, IntField>();
			String aggName = Aggregate.aggName(operator);
			Method method = AggregatorHandler.class.getMethod(aggName, fullList.getClass(), aggMap.getClass());
			method.invoke(new AggregatorHandler(),fullList, aggMap);
    	}
    	catch (NoSuchMethodException e) 
    	{
			System.out.println(e);
		}
    	catch (InvocationTargetException e)
    	{
    		System.out.println(e);
    	}
    	catch (IllegalAccessException e) 
    	{
    		System.out.println(e);
    	}
    }
}
