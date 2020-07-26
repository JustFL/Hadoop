package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 原始数据
 * 1        0        家电
 * 2        0        服装
 * 3        0        食品
 * 4        1        洗衣机
 * 5        1        冰箱
 * 10       4        美的
 * 11       5        海尔
 * 
 * 结果数据
 * 家电        洗衣机-美的    
 * 家电        冰箱-海尔   
 * @author summerKiss
 *
 */

class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String[] split = value.toString().split("\t");
		//是顶级父类
		if (split[1].equals("0")) {
			k.set(split[0]);
			v.set(split[1]+"\t"+split[2]);
			context.write(k, v);
		}else {
			k.set(split[1]);
			v.set(split[0]+"\t"+split[2]);
			context.write(k, v);
		}
	}
}

class MyReducer extends Reducer<Text, Text, Text, Text>{
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/**
		 * 这里为什么要这么做
		 * 首先发送过来的是这样的数据
		 * 1        0		家电
		 * 1        4		洗衣机
		 * 1        5		冰箱
		 * 
		 * key值代表什么 需要靠values的开始字符确定 
		 * 以0开头表示是顶级父类 key值是顶级父类的id
		 * 不是0开头的 key值代表父类id 
		 * 以key值相同的为一组 发送过来的数据中其实有父类也有子分类 需要进行拼接
		 * 
		 * 我们要做成这样的       
		 * 4        0		家电-洗衣机
		 * 5        0		家电-冰箱
		 * 拼接完成需要加入0 表示是顶级父类 以便进行循环
		 * 
		 * 问题是还有一些数据不是父类和子类混合的 
		 * e.g
		 * 4        10      美的
		 * 这个数据其实是家电的孙子类 是第二步才能进行拼接的 这个数据的分组中没有父类 需要单独讨论
		 * 
		 * 此外还有一种
		 * 9        3        水果
		 * 这个数据在第一步进行拼接完成后 会形成这样的
		 * 9		0        食品-水果
		 * 这个数据是没有子类的 只有父类 也需要单独讨论
		 *
		 * 基于以上分析的三种情况 在每一组过来的时候 我们需要首先进行遍历 查看组中父类和子类的存在情况 因为Iterable<Text>这个只能遍历一遍
		 * 所以建立容器将数据存储 而且还可以利用容器对是否存在父类进行判断
		 */
		String father = "";
		ArrayList<String> children = new ArrayList<String>();
		for (Text t : values) {
			String value = t.toString();
			//找到父类 
			if (value.startsWith("0")) {
				father = value;
			}else {
				children.add(value);
			}
		}
		/**
		 * 如果没有父类 将从map反转的数据还原 准备进行下一次map
		 * 4        10      美的
		 * 
		 */
		if (father.equals("")) {
			children.forEach(s->{
				k.set(s.split("\t")[0]);
				v.set(key+"\t"+s.split("\t")[1]);
				try {
					context.write(k, v);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			});
		}else if (children.size() == 0) {
			/**
			 * 如果只有父类
			 * 9		0        食品-水果
			 * 直接输出
			 */
			
			k.set(key);
			v.set(father);
			context.write(k, v);
			
		}else {
			/**
			 * 混合类型 进行父类名字和子类名字的拼接
			 * 
			 */
			
			for (String string : children) {
				String cid = string.split("\t")[0];
				String cname = string.split("\t")[1];
				String fname = father.split("\t")[1];
				k.set(cid);
				v.set("0"+"\t"+fname+"-"+cname);
				context.write(k, v);
			}
		}
	}
}



public class Interview1 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		int count = 0;
		while (true) {
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(Interview1.class);
			
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			if (count == 0) {
				FileInputFormat.addInputPath(job, new Path("/mr1"));
			}else {
				FileInputFormat.addInputPath(job, new Path("/mr1_0"+(count-1)));
			}
			
			FileOutputFormat.setOutputPath(job, new Path("/mr1_0"+count));
			
			job.waitForCompletion(true);
			
			count++;
			
			if (count == 3) {
				break;
			}
		}
		
		
	}
}
