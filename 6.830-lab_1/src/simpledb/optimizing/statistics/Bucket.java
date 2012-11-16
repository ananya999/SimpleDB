package simpledb.optimizing.statistics;

public class Bucket {
	double b_right;
	double b_left;
	int height;
	
	public Bucket(double b_left, double b_right, int height) {
		this.b_right = b_right;
		this.b_left = b_left;
		this.height = height;
	}
	
	int  getHeight()
	{
		return height;
	}
	
	double getWidth()
	{
		return b_right - b_left;
	}

	@Override
	public String toString() {
		return "Bucket [b_right=" + b_right + ", b_left=" + b_left
				+ ", height=" + height + "]";
	}
	
}
