package mapreduce;
/**
 * 
 * SelfDefineClass已经实现了自定义排序
 * 将SelfDefineClass作为key进行自定义排序
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
		 * 这里的key为自定义类型 每一个自定义对象的地址都不同 
		 * 但是分组不是按照对象的地址 而是按照自定义类的compareTo方法进行分组的 值相同的分成了一组
		 * 所以如果compareTo方法中比较的成员变量有相同的值 那么就会被分为一组 这样的话一组内就会有多个对象  所以必须遍历输出
		 * 换言之 如果是按照地址分组 因为地址都是不同的 所以每组中只可能有一个对象 就不需要遍历了
		 * 之前的代码使用的是基本类型 则调用的是基本类型的compareTo方法
		 * 可以类比set中放自定义类型时候的排序
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
		
		//将自定义类实验的输出结果作为输入
		FileInputFormat.addInputPath(job, new Path("/defineout01"));
		FileOutputFormat.setOutputPath(job, new Path("/definesortout04"));
		
		job.waitForCompletion(true);	
	}
}
