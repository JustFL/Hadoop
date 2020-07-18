package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.WordCountCounter.MyCounter;

/**
 *  ��ܼ���������˵��
 * File System Counters �ļ���д�ļ�����
		FILE: Number of bytes read=1510
		FILE: Number of bytes written=583048
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=1592
		HDFS: Number of bytes written=796
		HDFS: Number of read operations=13
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=4
	Map-Reduce Framework ��ܼ�����
		Map input records=21 map���������������
		Map output records=21 map���������������
		Map output bytes=522 map��������ֽ���
		Map output materialized bytes=570 map������Ĺ̻��ֽ���
		Input split bytes=116 �������Ƭ���ֽ���
		Combine input records=0 combine����������
		Combine output records=0 combine�������������
		Reduce input groups=21 ��reduce���ܹ��ж�����
		Reduce shuffle bytes=570 shuffle���̵��ֽ���
		Reduce input records=21 reduce�������Ŀ��
		Reduce output records=21 reduce�������Ŀ��
		Spilled Records=42 ��д�ļ�¼����
		Shuffled Maps =1 ����shuffle���̵�maptask����
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=9 �������յ�ʱ��
		Total committed heap usage (bytes)=362807296
	Shuffle Errors Shuffle������Ϣ
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=796
	File Output Format Counters 
		Bytes Written=796
 * */

/**
 * �Զ��������ͨ������ȫ�ֱ�����ʱ��ʹ��
 * ����ͳ�����ݵļ�¼�������ֶ���Ŀ
 * */
class WordCountCounter {
	enum MyCounter{
		lines,//��¼����
		count//��¼�ֶ���
	}
}

class CounterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//��ȡ������Ŀ�������� û��һ�����ݾͽ�������1
		Counter lines_counter = context.getCounter(MyCounter.lines);
		lines_counter.increment(1L);
		//��ȡ�����ֶμ����� ������ĳ����ۼ�
		Counter count_counter = context.getCounter(MyCounter.count);
		String[] split = value.toString().split("\t");
		count_counter.increment(split.length);
		context.write(NullWritable.get(), NullWritable.get());
	}	
}

public class TestCounter {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(TestCounter.class);
		
		job.setMapperClass(CounterMapper.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
	
		//��û������reduce����ʱ reducetask�ĸ���Ĭ��Ϊһ�� 
		//���ǵ�����Ҫreducetask��ʱ�� һ��Ҫ����Ϊ0
		//ע�����ļ�part-m-00000��maptask�Ľ���ļ� part-r-00000��reducetask�Ľ���ļ�
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path("/definein"));
		FileOutputFormat.setOutputPath(job, new Path("/mycounter_02"));
		
		job.waitForCompletion(true);
	}
}
