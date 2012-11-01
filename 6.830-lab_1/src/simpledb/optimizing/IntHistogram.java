package simpledb.optimizing;

import java.util.Arrays;

import simpledb.predicates.Predicate;
import simpledb.tuple.Field;
import simpledb.tuple.IntField;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {

	private Bucket[] segmentation;
	private double fixedRange;
	private int ntups = 0;
	private int max;
	private int min;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.max = max;
    	this.min = min;
    	double last = min;
    	int overAllRange = max - min;
    	fixedRange = (double)overAllRange/buckets;
    	segmentation = new Bucket[buckets];
		
    	for (int i = 0; i < segmentation.length; i++) 
    	{
    		segmentation[i] = new Bucket(last, last + fixedRange, 0);
    		last = last + fixedRange;
		}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(Field v) {
    	ntups++;
    	int bucket = findBucket(((IntField)v).getValue());
		segmentation[bucket].height++;
    }

    private int findBucket(int v) 
	{
		int bucket = -1;
		if (v >= max)
		{
			bucket = segmentation.length -1;
		}
		else if (v <= min)
		{
			bucket = 0;
		}
		else
		{
			double b = (v - min)/fixedRange;
			bucket = (int)Math.floor(b);
		}
		return bucket;
	}
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, Field f) 
    {
    	int v = ((IntField)f).getValue();
    	double selectivityFactor = 0;
    	int b = findBucket(v);
		Bucket bucket = segmentation[b];
		int h = bucket.getHeight();
		double w = bucket.getWidth();
		
    	switch (op) 
		{
			case EQUALS:
			{
				if (v < min || v > max)
				{
					selectivityFactor = 0;
				}
				else
				{
					selectivityFactor = (h / w) / ntups;
				}
				break;
			}
			case GREATER_THAN:
			{
				if (v > max)
				{
					selectivityFactor = 0;
				}
				else if (v < min)
				{
					selectivityFactor = 1;
				}
				else
				{
					double sum = bucket.getPartialFraction(v);
					for (int i = b + 1; i < segmentation.length; i++) 
					{
						sum = sum + segmentation[i].getFranction();
					}
					selectivityFactor = sum;
				}
				break;
			}
			case LESS_THAN:
			{
				if (v > max)
				{
					selectivityFactor = 1;
				}
				else if (v < min)
				{
					selectivityFactor = 0;
				}
				else
				{
					double sum = bucket.getPartialFraction(v);
					for (int i = b - 1; i >= 0; i--) 
					{
						sum = sum + segmentation[i].getFranction();
					}
					selectivityFactor = sum;
				}
				break;
			}
			case GREATER_THAN_OR_EQ:
			{
				if (v > max)
				{
					selectivityFactor = 0;
				}
				else if (v < min)
				{
					selectivityFactor = 1;
				}
				else
				{
					// GREATER_THAN
					double sum = bucket.getPartialFraction(v);
					for (int i = b + 1; i < segmentation.length; i++) 
					{
						sum = sum + segmentation[i].getFranction();
					}
					selectivityFactor = sum;
					
					// EQUALS
					selectivityFactor = selectivityFactor + (h / w) / ntups;
				}
				break;
			}
			case LESS_THAN_OR_EQ:
			{
				if (v > max)
				{
					selectivityFactor = 1;
				}
				else if (v < min)
				{
					selectivityFactor = 0;
				}
				else
				{
					// LESS_THAN
					double sum = bucket.getPartialFraction(v);
					for (int i = b - 1; i >= 0; i--) 
					{
						sum = sum + segmentation[i].getFranction();
					}
					selectivityFactor = sum;
					
					// EQUALS
					selectivityFactor = selectivityFactor + (h / w) / ntups;
				}
				break;
			}
			case NOT_EQUALS:
			{

				if (v < min || v > max)
				{
					selectivityFactor = 1;
				}
				else
				{
					selectivityFactor = 1 - (h / w) / ntups;
				}
				break;
			}
			default:
				break;
		}
    	
    	return selectivityFactor;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

    	return Arrays.toString(segmentation);
    }
    
    private class Bucket
    {
    	double b_right;
    	double b_left;
    	int height;
    	
    	public Bucket(double b_left, double b_right, int height) {
			this.b_right = b_right;
			this.b_left = b_left;
			this.height = height;
		}
    	
    	double getFranction()
    	{
    		return (double)height/ntups;
    	}
    	
    	int  getHeight()
    	{
    		return height;
    	}
    	
    	double getWidth()
    	{
    		return b_right - b_left;
    	}
    	
    	double getPartialFraction(int v)
    	{
    		return getFranction() * (b_right - v)/getWidth();
    	}
    	
		@Override
		public String toString() {
			return "Bucket [b_left=" + b_left + ", b_right=" + b_right
					+ ", height=" + height + ", toString()=" + super.toString()
					+ "]";
		}
    	
    }
}
