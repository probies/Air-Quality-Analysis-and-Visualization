import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class GroupMR {

	public class GroupMapper extends Mapper<Object, Text, Text, IntWritable> {
			
		public void map(Object key, Text value, Context 
				context)throws IOException, InterruptedException{
			
			String[] line = value.toString().split(",");
			String station = line[4];
			String polutant  = line[9];
			if(StringUtils.isNumeric(polutant))
				context.write(new Text(station), new IntWritable(Integer.parseInt(polutant)));
		}

	}

	public class GroupReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
			
			int sumpolutant = 0;
			int numItems = 0;
			for(IntWritable val : values) {
				sumpolutant += val.get();
				numItems += 1;
			}
			context.write(key, new IntWritable(sumpolutant/numItems));
		}
		
	}


	public static void main(String args[])throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "GroupMR");
		job.setJarByClass(GroupMR.class);
		job.setMapperClass(GroupMapper.class);
		job.setCombinerClass(GroupReducer.class);
		job.setReducerClass(GroupMRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}		