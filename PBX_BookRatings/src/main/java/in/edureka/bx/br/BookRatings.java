package in.edureka.bx.br;

import java.io.IOException;

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
 * This class objective is to run a job for parsing BookRatings.csv file
 * 
 * Input : BX-Ratings.csv
 * Format : Text delimited with ";"
 * "BookRating","ISBN";"Ratings"
 */
public class BookRatings {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public String cleanISBN(String corrupt) {
			
			// clean the corrupted ISBN's
			// remove all chars except 0-9, Alphabets
			
			String isbn = null;
			char[] isbnChar = corrupt.toCharArray();

			for(int i=0;i<isbnChar.length;i++){
			
				if (	Character.isDigit(isbnChar[i]) ||
						Character.isLetter(isbnChar[i])) {
					
					if (isbn == null)  {
						isbn = Character.toString(isbnChar[i]);
					} else {
						isbn = isbn+isbnChar[i];
					}
				}
			}

			return isbn;
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String record[] = value.toString().split("\";\"");
			// record[1] is ISBN.
			String isbn = cleanISBN(record[1]);
			// record[2] is rating.
			String rating = record[2].replace("\"", "").trim();
			
			// Invalid years such as 0 or not all digits between 0-9 are ignored
			if (isbn != null && !isbn.equalsIgnoreCase("isbn")) {
				if (rating.matches("[0-9]+")) {
					context.write(new Text(isbn),new IntWritable(Integer.parseInt(rating)));
				} else {
					System.out.println("BookRatings - Invalid BookRating encounted and ignored; (ISBN,BookRating)-["+isbn+","+rating+"]");
				}
			} else {
				System.out.println("BookRatings - Invalid ISBN encounted and ignored; (ISBN,BookRating)-["+isbn+","+rating+"]");
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			// Many books are rated multiple times. For this exercise, multiple rated book need to be counted one time only.
			// I am using there highest rating, as it doesn't matter for this exercise.
			
			int highestRating = 0;
			for(IntWritable val : values) {
				if (val.get() > highestRating ) highestRating = val.get();
			}

			context.write(key, new IntWritable(highestRating));
		}
	}

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(BookRatings.class);
		job.setJobName("BX_BookRatings");

		//set to do cleanup
		job.setJobSetupCleanupNeeded(true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
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
