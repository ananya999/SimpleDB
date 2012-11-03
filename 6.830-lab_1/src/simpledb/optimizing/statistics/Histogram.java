package simpledb.optimizing.statistics;

import simpledb.predicates.Predicate;
import simpledb.tuple.Field;

public interface Histogram {

	public void addValue(Field v);
	public double estimateSelectivity(Predicate.Op op, Field v);
	
}
