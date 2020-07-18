package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author zy
 * ����Ʒ��Ͷ�����������ϲ�ѯ��join����  ������reduce�˽���
 * �������ű�Ĺ����ֶ���pid ����Ҫ��pid��Ϊkey���з���
 * ���ű�������ֶ���Ϊvalue Ϊ����reduce�˶����ݽ������� ����Ҫ��map�˶����ݽ��д��ǩ
 * product:
 * P0001	iPhoneX	c01	2000
 * order:
 * 1001	20150710	P0001	2
 * 
 * ���ַ����ı׶�
 * 1 ���жȲ���
 * 2 ���ײ���������б ��Ϊ�����reducetask����ʱ ����Ĭ�ϵ�keyֵȡhash����з��� ����������Ʒ�Ķ����϶��� 
 * 3 �ܵ��������ݵ�������������Լ
 * */
public class ReduceJoin {
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		String fileName = new String();
		Text k = new Text();
		Text v = new Text();
		/**
		 * setupÿ��maptask����һ�� Ҳ����ÿһ����Ƭ����һ�� 
		 * �����ȡ�ļ������� ��Ϊ���ݵı�ǩ  Ϊ����reduce�˶����ݽ�������
		 * */
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			fileName = inputSplit.getPath().getName();
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] datas = value.toString().split("\t");
			if (fileName.equals("product.txt")) {
				k.set(datas[0]);
				v.set("PR"+datas[1]+"\t"+datas[2]+"\t"+datas[3]);
			}else {
				k.set(datas[2]);
				v.set("OR"+datas[0]+"\t"+datas[1]+"\t"+datas[3]);
			}
			context.write(k, v);
		}
	}
	
	static class MyReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		
		String content = null;
		String res = null;
		Text k = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			List<String> prlist = new ArrayList<>();
			List<String> orlist = new ArrayList<>();
			
			for (Text text : values) {
				content = text.toString();
				System.out.println("content:-------------"+content);
				if (content.startsWith("PR")) {
					prlist.add(content.substring(2));
				}else {
					orlist.add(content.substring(2));
				}
			}
			
			System.out.println(prlist.size());
			System.out.println("////////////////////////");
			System.out.println(orlist.size());
			
			//���жϷ�ֹ�е���Ʒû�ж��� ֻ����Ʒ�����м�¼ 
			if (prlist.size() > 0 && orlist.size() > 0) {
				//ÿһ����Ʒ�����ܶ�Ӧ������� ����join��ʱ�� ѭ������ȥƴ��ÿһ����Ʒ
				for (String order : orlist) {
					res = order + "\t" + prlist.get(0);
					k.set(key.toString()+"\t"+res);
					context.write(k, NullWritable.get());
				}
			}
			
				
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ReduceJoin.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path("/reducejoin"));
		FileOutputFormat.setOutputPath(job, new Path("/reducejoin_1"));
		
		job.waitForCompletion(true);
	}
}
