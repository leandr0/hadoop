package enemMedia;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MediaEnem {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			args = new String[2];
			args[0] = System.getProperty("user.dir")+File.separator+"Input";
			args[1] = System.getProperty("user.dir")+File.separator+"Output";
		}
		
		System.out.println(args[0]);
		System.out.println(args[1]);
		
		Job job = new Job();
		job.setJarByClass(MediaEnem.class);
		job.setJobName("Media das Notas do Enem por UF");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MediaEnemMapper.class);
		job.setReducerClass(MediaEnemReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
