import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
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



public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			
			// value = dear bear river 

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				
				context.write(value, new IntWritable(1));
			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		//K, list (values)
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			      throws IOException, InterruptedException {

			int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		//JobConf conf = new JobConf(WordCount.class);
		Configuration conf = new Configuration(true);
		
		conf.set("textinputformat.record.delimiter","\n\n");
		//Job job = Job.getInstance(new Configuration());
		Job job = new Job(conf);
		//job.setJobName("wordcount");
		
		//conf.setInt("mapred.line.input.format.linespermap", 10);
		//conf.set("mapred.textoutputformat.separator", ";");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/home/abhishek_linuxhadoop/workspace/MR_WordCount/SampleDataSet.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/abhishek_linuxhadoop/workspace/MR_WordCount/OUTPUT_Word_Count"));
		
		

		job.setJarByClass(WordCount.class);
		job.submit();

	}

}


