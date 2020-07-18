package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ȥ�� ����mapper���ͳ���key value���Զ�����keyֵ���з��������
 * ���Խ�ԭʼ����ֱ����Ϊkeyֵ����ȥ�ظ�
 * @author summerKiss
 *
 */

class DuplicateMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		context.write(value, NullWritable.get());
	}
}

class DuplicateReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	protected void reduce(Text key, java.lang.Iterable<NullWritable> values, Context context) throws java.io.IOException ,InterruptedException {
		context.write(key, NullWritable.get());
	}
}

public class RemoveDuplicate {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(RemoveDuplicate.class);
		
		job.setMapperClass(DuplicateMapper.class);
		job.setReducerClass(DuplicateReducer.class);
		
		//��mapper��reducer���������һ�µ�ʱ�� ���Խ�ָ��mapper�������͵����ʡ�Ե�
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/duplicateremove"));
		FileOutputFormat.setOutputPath(job, new Path("/duplicateout01"));
		
		job.waitForCompletion(true);
	}
}
