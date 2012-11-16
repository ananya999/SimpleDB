package simpledb.optimizing.statistics;

import java.util.Iterator;

import simpledb.predicates.Predicate;
import simpledb.tuple.Field;

public interface Histogram {

	public void addValue(Field v);
	public double estimateSelectivity(Predicate.Op op, Field v);
	public Iterator<Bucket> getBucketIterator();
	public int getHightOfRange(double b_left, double b_right);
	
}
