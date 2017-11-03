package Association;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class assoc_Main {

	public static void main(String[] ar) throws IOException, ClassNotFoundException, InterruptedException{
	Configuration conf = new Configuration();
	
	
	
	
	/**
	 * Job1
	 * */
	Job job1 = new Job(conf, "JobName");
	job1.setJarByClass(assoc_Main.class);   //What should be the name of the jar name after generated
	
	FileInputFormat.addInputPath(job1, new Path(ar[0]));   //locating the input
	job1.setInputFormatClass(TextInputFormat.class);
	
	job1.setMapperClass(assoc_Mapper.class);
	
	job1.setMapOutputKeyClass(Text.class); //what should the intermediate key data type is ? known by the framework from here
	job1.setMapOutputValueClass(Text.class); //same for the value as above
	
	job1.setNumReduceTasks(5);
	job1.setReducerClass(assoc_Reducer.class);
	
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);
	
	job1.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job1, new Path(ar[1]));
	
	job1.waitForCompletion(true);
	//if(!succeeded){
	//	throw new IllegalStateException("Job1 Failed");
	//}
	//create the input splits
	//assign each split to map task
	//map reads input split record by record like line by line
	
	
	/**
	 * Job1
	 * */
	Job job2 = new Job(conf, "JobName");
	job2.setJarByClass(assoc_Main.class);   //What should be the name of the jar name after generated
	
	FileInputFormat.addInputPath(job2, new Path(ar[1]));   //locating the input
	job2.setInputFormatClass(TextInputFormat.class);
	
	job2.setMapperClass(assoc_Mapper2.class);
	
	job2.setMapOutputKeyClass(Text.class); //what should the intermediate key data type is ? known by the framework from here
	job2.setMapOutputValueClass(Text.class); //same for the value as above
	
	job2.setNumReduceTasks(5);
	job2.setReducerClass(assoc_Reducer2.class);
	
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	
	job2.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job2, new Path(ar[2]));
	
	job2.waitForCompletion(true);
	
	/**
	 * Job 3
	 * */
	
	Job job3 = new Job(conf, "JobName");
	job3.setJarByClass(assoc_Main.class);   //What should be the name of the jar name after generated
	
	FileInputFormat.addInputPath(job3, new Path(ar[2]));   //locating the input
	job3.setInputFormatClass(TextInputFormat.class);
	
	job3.setMapperClass(assoc_Mapper3.class);
	
	job3.setMapOutputKeyClass(Text.class); //what should the intermediate key data type is ? known by the framework from here
	job3.setMapOutputValueClass(Text.class); //same for the value as above
	
	job3.setNumReduceTasks(5);
	job3.setReducerClass(assoc_Reducer3.class);
	
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(Text.class);
	
	job3.setOutputFormatClass(TextOutputFormat.class);
	FileOutputFormat.setOutputPath(job3, new Path(ar[3]));
	
	boolean succeeded3 = job3.waitForCompletion(true);

	if(!succeeded3){
			throw new IllegalStateException("Job3 Failed");
		}
	
	
	
	
	
	
	
	
	
	}	
}
