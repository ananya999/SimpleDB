package simpledb.aggregates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import simpledb.predicates.Predicate.Op;
import simpledb.tuple.Field;
import simpledb.tuple.IntField;

public class AggregatorHandler {

	
	public void sum(ArrayList<Entry<Field, Field>> full, HashMap<Field, IntField> aggMap)
	{
		for (Entry<Field, Field> entry : full) 
		{
			Field k = entry.getKey();
			IntField aggv = aggMap.get(k);
			Field v = entry.getValue();
			if (aggv == null)
			{
				aggMap.put(k, (IntField)v);
			}
			else
			{
				aggMap.put(k, new IntField(((IntField)v).getValue() + aggv.getValue()));
			}
		}
	}
	
	public void max (ArrayList<Entry<Field, Field>> full, HashMap<Field, IntField> aggMap)
	{
		for (Entry<Field, Field> entry : full) 
		{
			Field k = entry.getKey();
			IntField aggv = aggMap.get(k);
			Field v = entry.getValue();
			if (aggv == null)
			{
				aggMap.put(k, (IntField)v);
			}
			else if (aggv.compare(Op.LESS_THAN, v))
			{
				aggMap.put(k, (IntField)v);
			}
		}
	}
	
	public void min (ArrayList<Entry<Field, Field>> full, HashMap<Field, IntField> aggMap)
	{
		for (Entry<Field, Field> entry : full) 
		{
			Field k = entry.getKey();
			IntField aggv = aggMap.get(k);
			Field v = entry.getValue();
			if (aggv == null)
			{
				aggMap.put(k, (IntField)v);
			}
			else if (aggv.compare(Op.GREATER_THAN, v))
			{
				aggMap.put(k, (IntField)v);
			}
		}
	}
	
	public void count (ArrayList<Entry<Field, Field>> full, HashMap<Field, IntField> aggMap)
	{
		for (Entry<Field, Field> entry : full) 
		{
			Field k = entry.getKey();
			IntField aggv = aggMap.get(k);
			if (aggv == null)
			{
				aggMap.put(k, new IntField(1));
			}
			else
			{
				aggMap.put(k, new IntField(aggv.getValue() + 1));
			}
		}
	}
	public void avg (ArrayList<Entry<Field, Field>> full, HashMap<Field, IntField> aggMap)
	{
		Map<Field, Integer> counter = new HashMap<Field, Integer>();
		for (Entry<Field, Field> entry : full) 
		{
			Field k = entry.getKey();
			IntField aggv = aggMap.get(k);
			Field v = entry.getValue();
			if (aggv == null)
			{
				aggMap.put(k, (IntField)v);
				counter.put(k, 1);
			}
			else
			{
				Integer c = counter.get(k);
				aggMap.put(k, new IntField((((IntField)v).getValue() + c*aggv.getValue())/(c + 1)));
				counter.put(k,c + 1);
				
			}
		}
		System.out.println("felix");
	}
	
}
