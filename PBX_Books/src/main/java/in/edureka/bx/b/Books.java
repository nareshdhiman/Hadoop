package in.edureka.bx.b;

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
 * This class objective is to run a job for parsing Books.csv file
 * 
 * Input : BX-Books.csv
 * Format : Text delimited with ";"
 * "ISBN";"Book-Title";"Book-Author";"Year-Of-Publication";"Publisher";"Image-URL-S";"Image-URL-M";"Image-URL-L"
 */
public class Books {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
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
			String isbn = cleanISBN(record[0]);
			// record[2] is rating.
			String publishedYear = record[3].replace("\"", "").trim();
			
			// Invalid years such as 0 or not all digits between 0-9 are ignored
			if (isbn != null && !isbn.equalsIgnoreCase("isbn")) {
				
				// Invalid years such as 0 or not all digits between 0-9 are ignored
				if (publishedYear.matches("[0-9]+") && !publishedYear.equals("0")) {
					context.write(new Text(isbn), new Text(publishedYear));
				} else {
					System.out.println("Books - Invalid published year encounted and ignored;(ISBN,YearOfPublication)-["+isbn+","+publishedYear+"]");
				}
			} else {
				System.out.println("BookRatings - Invalid ISBN encounted and ignored; (ISBN,YearOfPublication)-["+isbn+","+publishedYear+"]");
			}
		}
	}

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Books.class);
		job.setJobName("BX_BookRatings");

		//set to do cleanup
		job.setJobSetupCleanupNeeded(true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MyMapper.class);
		//job.setReducerClass(MyReducer.class);
		
		//Defining classes for the mapper		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Defining the output key class for the final output i.e. from reducer
		
		//job.setOutputKeyClass(Text.class);		
		//job.setOutputValueClass(Text.class);
		
		//Defining input Format class which is responsible to parse the 
		//dataset into a key value pair 
		job.setInputFormatClass(TextInputFormat.class);
				
		//Defining output Format class which is responsible to parse the 
		//final key-value output from MR framework to a text file into the hard disk
		job.setOutputFormatClass(TextOutputFormat.class);
				
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}
