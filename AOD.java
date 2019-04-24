import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AOD {

	public static class AodMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable ageOfDriver = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), ",");

			for (int i = 0; i < 16; i++) {
				itr.nextToken();
			}

			ageOfDriver.set(Integer.parseInt(itr.nextToken()));
			
			if(ageOfDriver.get() != -1 && 
				ageOfDriver.get() != 1 && 
				ageOfDriver.get() != 2 && 
				ageOfDriver.get() != 3)
			{				
				context.write(ageOfDriver, one);
			}
			

		}
	}

	public static class AodReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AgeOfDriverCount <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Age of driver");
		job.setJarByClass(AOD.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(AodMapper.class);
		job.setCombinerClass(AodReducer.class);
		job.setReducerClass(AodReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
