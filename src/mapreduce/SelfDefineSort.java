package mapreduce;
/**
 * 
 * SelfDefineClass�Ѿ�ʵ�����Զ�������
 * ��SelfDefineClass��Ϊkey�����Զ�������
 * @author summerKiss
 *
 */

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

class DefineSortMapper extends Mapper<LongWritable, Text, Flowbean, NullWritable>{
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String[] split = value.toString().split("\t");
		Flowbean fb = new Flowbean(Integer.parseInt(split[1].trim()), Integer.parseInt(split[2].trim()));
		context.write(fb, NullWritable.get());
	}
}

class DefineSortReducer extends Reducer<Flowbean, NullWritable, Flowbean, NullWritable>{
	protected void reduce(Flowbean key, java.lang.Iterable<NullWritable> values, Context context) throws java.io.IOException ,InterruptedException {
				
		/**
		 * �����keyΪ�Զ������� ÿһ���Զ������ĵ�ַ����ͬ 
		 * ���Ƿ��鲻�ǰ��ն���ĵ�ַ ���ǰ����Զ������compareTo�������з���� ֵ��ͬ�ķֳ���һ��
		 * �������compareTo�����бȽϵĳ�Ա��������ͬ��ֵ ��ô�ͻᱻ��Ϊһ�� �����Ļ�һ���ھͻ��ж������  ���Ա���������
		 * ����֮ ����ǰ��յ�ַ���� ��Ϊ��ַ���ǲ�ͬ�� ����ÿ����ֻ������һ������ �Ͳ���Ҫ������
		 * ֮ǰ�Ĵ���ʹ�õ��ǻ������� ����õ��ǻ������͵�compareTo����
		 * �������set�з��Զ�������ʱ�������
		 * */
		
		System.out.println("=============");
		for (NullWritable nl : values) {
			System.out.println(key);
			context.write(key, nl);
		}
	}
}

public class SelfDefineSort {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SelfDefineSort.class);
		
		job.setMapperClass(DefineSortMapper.class);
		job.setReducerClass(DefineSortReducer.class);
		
		job.setOutputKeyClass(Flowbean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//���Զ�����ʵ�����������Ϊ����
		FileInputFormat.addInputPath(job, new Path("/defineout01"));
		FileOutputFormat.setOutputPath(job, new Path("/definesortout04"));
		
		job.waitForCompletion(true);	
	}
}
