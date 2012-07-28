package simpledb;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		
		long time = new Date().getTime();
		
		List<Date> a_list= new ArrayList<Date>();
		for (int i = 0; i < time * 4; i++) 
		{
			a_list.add(new Date());
		}
	}

}
