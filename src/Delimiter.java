import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by antoine on 05/06/14.
 */
public class Delimiter {

	static File outputDir;
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter","\n\n");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Delimiter.class);

        FileInputFormat.addInputPath(job, new Path("/home/abhishek_linuxhadoop/workspace/MR_WordCount/AmazonMetadata.txt"));
        outputDir = new File("/home/abhishek_linuxhadoop/workspace/MR_WordCount/OUTPUT_Word_Count");
        if(outputDir.exists())
        {
        	if(deleteDir(outputDir))
        	{
        FileOutputFormat.setOutputPath(job, new Path("/home/abhishek_linuxhadoop/workspace/MR_WordCount/OUTPUT_Word_Count"));
        	}
        }
        else
        {
        FileOutputFormat.setOutputPath(job, new Path("/home/abhishek_linuxhadoop/workspace/MR_WordCount/OUTPUT_Word_Count"));
        }

        job.setMapperClass(DelimiterMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setInputFormatClass(MyInputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }


    public static class DelimiterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    //public static class DelimiterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private Text TEXT = new Text();
        String[] rowSplitted;
        int rowCount;
        int index;
        StringBuilder sb = new StringBuilder();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

        	try
        	{
            // Should be a 2 lines key value
        	//context.write(value.getValue1(), value.getValue2());
        	sb.setLength(0);
        	if(!(value.toString().contains("# Full information about Amazon Share the Love products")
        			|| value.toString().contains("discontinued product")))
        	{
        		rowCount = 0;
        		rowSplitted = value.toString().split("\n");
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].indexOf("Id:") + 3));
        		rowCount +=2;
//        		index = rowSplitted[++rowCount].indexOf("title:");
//        		sb.append(index);
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].indexOf("title:") + 6).trim());
        		
        		
        		
        		
        		
        		
        		
        	TEXT.set(sb.toString());
        	//	TEXT.set("******************" + rowSplitted[0]);
            context.write(NullWritable.get(), TEXT);
            //context.write(NullWritable.get(), TEXT);
        	}
        	System.gc();
        	//TEXT = null;
        	}
        	catch(Exception ex)
        	{
        		TEXT.set(")()*()*&*(&(*&()*((*()*" + ex.getMessage().toString());
        		context.write(NullWritable.get(), TEXT);
        	}
        	}
        
        public StringBuilder setData(String data) {
			
        	if(!data.isEmpty())
        	{
        		data.trim();
        		sb.append(data);
        		sb.append("=");
        	}
        	return sb;
		}
        
        
    
    }
    }