package Association;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

public class assoc_Reducer2 extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException
	{
	//	String[] arr = key.toString().split("\\s"); // stores the key
		
		ArrayList<String> list = new ArrayList<String>(); // stores all the values
		
		for(Text val : value)
		{
		 //   if(notFound(list,val.toString())==1)
			
			// make a deep copy
			Configuration conf = new Configuration();
			Text n = ReflectionUtils.newInstance(Text.class, conf);
			ReflectionUtils.copy(conf, val, n);
			list.add(val.toString());  // feed all values in the array 
		
		
			// check for any duplications (Important !!!!!)
		
		
		}
		
		ArrayList<String> combinations = getCombinations(list);
		
		
		for(int i=0;i<combinations.size();i++)
		{
			try {
				context.write(new Text(combinations.get(i)), new Text(key));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	/**
	 * 
	 * 
	 * A function to get all combinations of strings in an array list
	 * 
	 * 
	 * */
	public ArrayList<String> getCombinations(ArrayList<String> list)
	{
		ArrayList<String> combo = new ArrayList<String>();
		
		int size = list.size(); // gets the size of the list
		HashMap<Integer,Integer> hmap = new HashMap<Integer,Integer>();
		
		
		
		for(int i=0;i<size;i++)
		{
			for(int j=i+1;j<size;j++)
			{
				hmap.put(new Integer(list.get(i)), new Integer(list.get(j)));
			}
		}
		
		// returns set view
		Set<Map.Entry<Integer,Integer>> st = hmap.entrySet();
		
		// store all combinations in the arraylist
		for(Map.Entry<Integer,Integer> me : st)
		{
			String val="";
			val += me.getKey().toString();
			val +=",";
			val +=me.getValue().toString();
			combo.add(val);
		}
		
		return combo;
		
	}
	
	
    public int notFound(ArrayList<String> list,String c)
    {
    	
    	for(int i=0;i<list.size();i++)
    	{
    		if(list.get(i).equalsIgnoreCase(c))
    			return 0;
    	}
    	
    	return 1;
    }
	
	
	
}
