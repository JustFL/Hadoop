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
 *  框架计数器内容说明
 * File System Counters 文件读写的计数器
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
	Map-Reduce Framework 框架计数器
		Map input records=21 map端输入的数据条数
		Map output records=21 map端输出的数据条数
		Map output bytes=522 map端输出的字节数
		Map output materialized bytes=570 map端输出的固化字节数
		Input split bytes=116 输入的切片的字节数
		Combine input records=0 combine的数据条数
		Combine output records=0 combine输出的数据条数
		Reduce input groups=21 到reduce端总共有多少组
		Reduce shuffle bytes=570 shuffle过程的字节数
		Reduce input records=21 reduce输入的条目数
		Reduce output records=21 reduce输出的条目数
		Spilled Records=42 溢写的记录条数
		Shuffled Maps =1 经过shuffle过程的maptask个数
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=9 垃圾回收的时间
		Total committed heap usage (bytes)=362807296
	Shuffle Errors Shuffle错误信息
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
 * 自定义计数器通常在有全局变量的时候使用
 * 这里统计数据的记录条数和字段数目
 * */
class WordCountCounter {
	enum MyCounter{
		lines,//记录行数
		count//记录字段数
	}
}

class CounterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//获取数据条目数计数器 没有一行数据就进行增加1
		Counter lines_counter = context.getCounter(MyCounter.lines);
		lines_counter.increment(1L);
		//获取数据字段计数器 将数组的长度累加
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
	
		//当没有设置reduce个数时 reducetask的个数默认为一个 
		//但是当不需要reducetask的时候 一定要设置为0
		//注意结果文件part-m-00000是maptask的结果文件 part-r-00000是reducetask的结果文件
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job, new Path("/definein"));
		FileOutputFormat.setOutputPath(job, new Path("/mycounter_02"));
		
		job.waitForCompletion(true);
	}
}
