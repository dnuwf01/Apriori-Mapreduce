package Association;



import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class assoc_Mapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable offset, Text value, Context context) throws NumberFormatException
	{
		//long num;
		String key;
		String[] str = value.toString().split("\\s");
		
		
		for(int i=0;i<str.length;i++)
		{
		
			// num = Long.parseLong(str[i].toString());
			key = str[i];
	
		
			try {
				context.write(new Text(key), new Text(value));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
