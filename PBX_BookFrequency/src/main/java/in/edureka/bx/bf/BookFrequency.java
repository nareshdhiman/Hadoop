package in.edureka.bx.bf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * This class objective is to run a job for finding frequency of the books published 
 * each year.
 * 
 * Input : BX-Books.csv
 * Format : Text delimited with ";"
 * "ISBN";"Book-Title";"Book-Author";"Year-Of-Publication";"Publisher";"Image-URL-S";"Image-URL-M";"Image-URL-L"
 */
public class BookFrequency {
	
	public static class BFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String record[] = value.toString().split("\";\"");
			// record[3] is Published Year.
			String publishedYear = record[3].replace("\"", "").trim();
			
			// Invalid years such as 0 or not all digits between 0-9 are ignored
			if (publishedYear.matches("[0-9]+") && !publishedYear.equals("0")) {
				context.write(new Text(publishedYear),new IntWritable(1));
			} else {
				System.out.println("BookFrequency - Invalid published year encounted and ignored : "+publishedYear);
			}
		}
	}

	public static class BFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		String hightiestPubYear =null;
		int hightiestBooks = 0;
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int freq = 0;
			for (IntWritable val: values) {
				freq++;
			}
			
			if (hightiestBooks < freq) { hightiestBooks = freq; hightiestPubYear = key.toString();}
			
			context.write(key, new IntWritable(freq));
		}

		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			System.out.println("BookFrequency - Hightiest Books Published : "+hightiestBooks+" Year : "+hightiestPubYear);
			//context.write(new Text("Max Books Published Year : "+hightiestPubYear), new IntWritable(hightiestBooks));
			super.cleanup(context);
		}

		@Override
		protected void setup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
	}

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(BookFrequency.class);
		job.setJobName("BX_BookFrequency");

		//set to do cleanup
		job.setJobSetupCleanupNeeded(true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(BFMapper.class);
		job.setReducerClass(BFReducer.class);
		
		//Defining classes for the mapper		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//Defining the output key class for the final output i.e. from reducer
		
		job.setOutputKeyClass(Text.class);		
		job.setOutputValueClass(IntWritable.class);
		
		//Defining input Format class which is responsible to parse the 
		//dataset into a key value pair 
		job.setInputFormatClass(TextInputFormat.class);
				
		//Defining output Format class which is responsible to parse the 
		//final key-value output from MR framework to a text file into the hard disk
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}
