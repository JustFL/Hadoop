package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zy
 * map�˵�join���� ˼·��һ�Ŵ���һ��С�� ��С����ص��ڴ���
 * ��map������ֻ�Դ����ж�ȡ
 * 
 */

public class MapJoin {
	static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		//����һ��������С������ݽ��д洢
		HashMap<String, String> product = new HashMap<String, String>();
		
		Text k = new Text();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			//��ȡ�ڴ��д�ŵ�С���·��
			Path[] cacheFiles = context.getLocalCacheFiles();
			String path = cacheFiles[0].toString();
			System.out.println("path:-----------"+path);
			//��С������ݴ洢��������
			BufferedReader br = new BufferedReader(new FileReader(path));
			String str = null;
			while ((str = br.readLine()) != null) {
				String[] datas = str.split("\t");
				product.put(datas[0], datas[1]+"\t"+datas[2]+"\t"+datas[3]);
			}
			br.close();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String[] datas = value.toString().split("\t");
			String pid = datas[2];
			if (product.containsKey(pid)) {
				k.set(value.toString()+"\t"+product.get(pid));
				context.write(k, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapJoin.class);
		
		job.setMapperClass(MyMapper.class);
		
		//��С���ȡ���ڴ���
		job.addCacheFile(new URI("/reducejoin/product.txt"));
		
		//���û��reduce ���ָ�ľ���map�˵����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//���û��reduce ����ָ��Ϊ0�� ����Ĭ����һ��
		job.setNumReduceTasks(0);

		//�������ֻ��Ҫ���ش��
		FileInputFormat.addInputPath(job, new Path("/reducejoin/order.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/reducejoin_2"));
		
		job.waitForCompletion(true);
		
		//ֻ�ܴ�jar������
	}
}
