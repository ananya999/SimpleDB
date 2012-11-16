package simpledb.optimizing.statistics;

import java.util.Iterator;

import simpledb.Catalog;
import simpledb.Database;
import simpledb.logicalplan.LogicalJoinNode;

public class JoinCardinalityEstimator {
	
	private LogicalJoinNode j; 
	private TableStats stats1; 
	private TableStats stats2;
	
	public JoinCardinalityEstimator(LogicalJoinNode j, TableStats stats1, TableStats stats2) 
	{
		this.j = j;
		this.stats1 = stats1;
		this.stats2 = stats2;
	}

	public int estimateJoinCardinality()
	{
		int joinCardinality = 0;
		
		Catalog catalog = Database.getCatalog();
		
		int field1 = catalog.getTupleDesc(j.t1).nameToId(j.f1);
		int field2 = catalog.getTupleDesc(j.t2).nameToId(j.f2);

		Histogram h1 = stats1.getHistogram(field1);
		Iterator<Bucket> iter1 = h1.getBucketIterator();
		while(iter1.hasNext())
		{
			Bucket b1 = iter1.next();
			Histogram h2 = stats2.getHistogram(field2);
			int hight = h2.getHightOfRange(b1.b_left, b1.b_right);
			joinCardinality = joinCardinality + Math.min(hight, b1.height);
		}
		
		return joinCardinality;

	}
	
}
