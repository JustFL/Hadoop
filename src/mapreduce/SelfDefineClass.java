package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  内容:
 *  1>自定义类Flowbean
 *  2>自定义分区规则
 *  3>Flowbean类自定义排序规则
 * */
class Flowbean implements WritableComparable<Flowbean>{
	private int upflow;
	private int downflow;
	private int sumflow;
	
	public int getUpflow() {
		return upflow;
	}
	public void setUpflow(int upflow) {
		this.upflow = upflow;
	}
	public int getDownflow() {
		return downflow;
	}
	public void setDownflow(int downflow) {
		this.downflow = downflow;
	}
	public int getSumflow() {
		return sumflow;
	}
	public void setSumflow(int sumflow) {
		this.sumflow = sumflow;
	}
	
	@Override
	public String toString() {
		return upflow + "\t" + downflow + "\t" + sumflow;
	}
	
	public Flowbean() {
		super();
	}
	
	public Flowbean(int upflow, int downflow) {
		super();
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = this.upflow + this.downflow;
	}
	
	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readInt();
		this.downflow = in.readInt();
		this.sumflow = in.readInt();
	}
	
	//序列化方法 注意序列化和反序列化的顺序必须相同
	@Override
	public void write(DataOutput out) throws IOException {
		//这里特别注意不要用write方法！！！
		out.writeInt(upflow);
		out.writeInt(downflow);
		out.writeInt(sumflow);
	}
	
	@Override
	public int compareTo(Flowbean o) {
		//先按照总流量倒序排序 再按照上行流量倒序排序
		if (o.sumflow != this.sumflow) {
			return o.sumflow - this.sumflow;
		}else {
			return o.upflow - this.upflow;
		}
	}
}

class DefineMapper extends Mapper<LongWritable, Text, Text, Flowbean>{
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException ,InterruptedException {
		String[] split = value.toString().split("\t");
		Flowbean fb = new Flowbean(Integer.parseInt(split[split.length-3].trim()), Integer.parseInt(split[split.length-2].trim()));
		//验证手机号长度
		if (split[1].length() == 11) {
			context.write(new Text(split[1]), fb);
		}
	}
}

class DefineReducer extends Reducer<Text, Flowbean, Text, Flowbean>{
	protected void reduce(Text key, java.lang.Iterable<Flowbean> values, Context context) 
			throws IOException ,InterruptedException {
		int sum_upflow = 0;
		int sum_downflow = 0;
		for (Flowbean flowbean : values) {
			sum_upflow += flowbean.getUpflow();
			sum_downflow += flowbean.getDownflow();
		}
		
		Flowbean fb = new Flowbean(sum_upflow, sum_downflow);
		context.write(key, fb);
	}
}

class DefinePartition extends Partitioner<Text, Flowbean>{

	@Override
	public int getPartition(Text key, Flowbean value, int numPartitions) {
		String location = key.toString().substring(0, 3);
		if (location.equals("135") || location.equals("136")) {
			return 0;
		}else {
			return 1;
		}	
	}
	
}
public class SelfDefineClass {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(SelfDefineClass.class);
		
		job.setMapperClass(DefineMapper.class);
		job.setReducerClass(DefineReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Flowbean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Flowbean.class);
		
		//job.setNumReduceTasks(2);
		//job.setPartitionerClass(DefinePartition.class);
		
		FileInputFormat.addInputPath(job, new Path("/definein"));
		FileOutputFormat.setOutputPath(job, new Path("/defineout01"));
		
		job.waitForCompletion(true);
	}
}
