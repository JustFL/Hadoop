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
 * map端的join操作 思路是一张大表和一张小表 将小表加载到内存中
 * 在map函数中只对大表进行读取
 * 
 */

public class MapJoin {
	static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		//创建一个容器对小表的数据进行存储
		HashMap<String, String> product = new HashMap<String, String>();
		
		Text k = new Text();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			//获取内存中存放的小表的路径
			Path[] cacheFiles = context.getLocalCacheFiles();
			String path = cacheFiles[0].toString();
			System.out.println("path:-----------"+path);
			//将小表的数据存储到容器中
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
		
		//将小表读取到内存中
		job.addCacheFile(new URI("/reducejoin/product.txt"));
		
		//如果没有reduce 这个指的就是map端的输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//如果没有reduce 必须指定为0个 否则默认有一个
		job.setNumReduceTasks(0);

		//这里加载只需要加载大表
		FileInputFormat.addInputPath(job, new Path("/reducejoin/order.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/reducejoin_2"));
		
		job.waitForCompletion(true);
		
		//只能打jar包运行
	}
}
