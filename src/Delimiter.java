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

        String outpath = "/home/user/workspace/MR_AmazonDataAnalysis/OUTPUT_Word_Count";
        FileInputFormat.addInputPath(job, new Path("/home/user/workspace/MR_AmazonDataAnalysis/AmazonMetadata.txt"));
        outputDir = new File(outpath);
        if(outputDir.exists())
        {
        	if(deleteDir(outputDir))
        	{
        FileOutputFormat.setOutputPath(job, new Path(outpath));
        	}
        }
        else
        {
        FileOutputFormat.setOutputPath(job, new Path(outpath));
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
        String[] similarProductsRow;
        int rowCount = 0;
        int downloadedCount = 0;
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
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("Id:") + 3));
        		rowCount +=2;
//        		index = rowSplitted[++rowCount].indexOf("title:");
//        		sb.append(index);
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("title:") + 6).trim());
        		rowCount+=1;
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("group:") + 6).trim());
        		rowCount+=1;
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("salesrank:") + 10).trim());
        		rowCount+=1;
        		similarProductsRow = rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("similar:") + 8).split("  ");
        		this.setData(similarProductsRow[0].trim());
        		rowCount+=1;
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("categories:") + 11).trim());
        		rowCount+=Integer.parseInt(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("categories:") + 11).trim()) + 1;
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("total:") + 6,rowSplitted[rowCount].
        				indexOf("downloaded:")).trim());
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("downloaded:") + 11,rowSplitted[rowCount].
        				indexOf("avg")).trim());
        		this.setData(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("avg rating:") + 11).trim());
        		downloadedCount = Integer.parseInt(rowSplitted[rowCount].substring(rowSplitted[rowCount].
        				indexOf("downloaded:") + 11,rowSplitted[rowCount].
        				indexOf("avg")).trim());
        		rowCount+=1;
        		for(int i = rowCount;i<downloadedCount+rowCount;i++)
        		{
        			this.setData(rowSplitted[i].substring(rowSplitted[i].
            				indexOf("cutomer:") + 8,rowSplitted[i].
            				indexOf("rating:")).trim(),"|");
        		}
        		sb.append("|");
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
        		TEXT.set(")()*()*&*(&(*&()*((*()*" + ex.getMessage().toString()+sb);
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
public StringBuilder setData(String data,String delimiter) {
			
        	if(!data.isEmpty())
        	{
        		data.trim();
        		sb.append(delimiter);
        		sb.append(data);
        	}
        	return sb;
		}
        
        
    
    }
    }