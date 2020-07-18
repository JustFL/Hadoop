package mapreduce;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ��wordcount�����ͳ�ƽ�����մ�Ƶ���е�������
 * ��map��reduce������ ��Ĭ�ϵİ���keyֵ���е��������� 
 * ʵ�ַ������ǽ�ͳ�ƽ����key��value���л���
 * ����Ƶ��Ϊkey ������Ϊvalue �ٴν���һ��map��reduce�Ĺ��̽�������
 * ��������ʱ���ٷ�ת����
 * */
public class WordCountSort {
	static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//�����ԭʼ�����Ǿ�����Ƶͳ�ƵĽ�� Ĭ�Ϸָ�����\t
			String[] datas = value.toString().split("\t");
			String word = datas[0];
			int count = Integer.parseInt(datas[1]);
			//����Ƶ��Ϊkey ��������Ϊvalue���з��� ��ΪĬ�������� ����ȡ�������з���
			context.write(new IntWritable(-count), new Text(word));
			
		}
	}
	
	static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable>{
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			//���ڴ�Ƶ�����ظ� ������Ҫ���� ����Ƶȡ�෴������
			for (Text t : values) {
				context.write(t, new IntWritable(-key.get()));
			}
		}
	} 
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//�ص�:�����ύ������û� һ��Ҫ����ָ�� 
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		//��ȡ�����ļ�
		Configuration conf = new Configuration();
		//����һ��job ������װmapper��reducer
		Job job = Job.getInstance(conf);
		
		//������������
		job.setJarByClass(WordCountSort.class);
		
		//����mapper��reducer
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		//����mapper���������
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//����reducer���������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.199.10:9000/wordout03"));
		//�������·�� ���·�����ܴ��� ����½�ԭ�����ļ�����
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.199.10:9000/sort01"));	
		
		job.waitForCompletion(true);
	}
}
