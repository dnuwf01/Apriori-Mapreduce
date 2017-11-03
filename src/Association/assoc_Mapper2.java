package Association;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class assoc_Mapper2 extends Mapper<LongWritable,Text,Text,Text>{


	public void map(LongWritable offset, Text value, Context context){
	
		
		String str[] = value.toString().split("\\s+");
		String key=""; //key
		String val=""; //value
		
		
		
		for(int i=0;i<str.length;i++){
			if(i==0) key+=str[i];
			else
				val=val + str[i]+" ";
		}
		
		val = val.trim(); // remove trailing space
		
		try {
			context.write(new Text(val), new Text(key));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
}
