package Association;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

public class assoc_Reducer3 extends Reducer<Text,Text,Text,Text>{

	
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		//write code
		
		long total = 0;
	    ArrayList<Text> list = new ArrayList<Text>();
		
		for (Text val : value){
		total++;
		
	//	context.write(new Text(key), new Text(val));
		//if(notFound(list,val)==1){
			
		context.write(key, val);
		//create a deep copy
		Configuration conf = new Configuration();
		Text n = ReflectionUtils.newInstance(Text.class, conf);
		ReflectionUtils.copy(conf, val, n);
		
		list.add(n);
		
		}
		
	//	for(int i=0;i<list.size();i++)
		//	context.write(key, new Text(list.get(i)));
		
		
		//System.out.println("Total is "+total);
	/*	
		if(total>20){
		
			for (int i=0;i<list.size();i++){
			try {
			context.write(new Text(key), new Text(list.get(i)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // end catch
			}// end for
		}// end if
		
		*/
		}// end reduce
	
	
	    public int notFound(ArrayList<Text> list,Text c)
	    {
	    	
	    	for(int i=0;i<list.size();i++)
	    	{
	    		if(list.get(i).compareTo(c) == 0)
	    			return 0;
	    	}
	    	
	    	return 1;
	    }
	
}// end class

