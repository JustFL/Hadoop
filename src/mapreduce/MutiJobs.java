package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * 多Job的串联 使用求共同好友来示例
 * 数据:
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * A关注了B,C,D,F,E,O
 * B关注了A,C,E,K
 * 求A和B共同关注的有谁
 * 第一步求出每一个人都被谁关注 例如A被B关注 结果应该是A被X,X,X,X关注
 * 第二部利用第一步的结果将所有的X进行两两拼接 然后反转就是结果
 */



class Mapper_step01 extends Mapper<LongWritable, Text, Text, Text>{
	
	
	Text k = new Text();
	Text v = new Text();
	/**
	 * 这里可以优化代码 Mapper类是每一个maptask任务对应一个 但是map方法是每一行数据调用一次  
	 * 所以将new Text()的动作放在mapper类中 否则会创建过多的对象
	 * */
	protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException ,InterruptedException {
		//两个元素 用户A 用户关注的对象B,C,D,F,E,O
		String[] split = value.toString().split(":");
		String person = split[0];
		//将关注的对象列表切分
		String[] friends = split[1].split(",");
		for (String f : friends) {
			k.set(f);
			v.set(person);
			//打包成B:A C:A D:A F:A ... 进行发送
			context.write(k, v);
		}
	}
}

class Reducer_step01 extends Reducer<Text, Text, Text, Text>{
	Text v = new Text();
	
	//接受的元素 用户B 被A等一系列对象关注 将这一系列对象拿出来 加入,拼接
	protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) 
			throws java.io.IOException ,InterruptedException {
		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			sb.append(text.toString()+",");
		}
		
		v.set(sb.substring(0, sb.length()-1));
		//发送形式 B A,C,F等
		context.write(key, v);
	}
}


class Mapper_step02 extends Mapper<LongWritable, Text, Text, Text>{
	Text k = new Text();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//数据格式B A,C,F 对关注B的一系列用户 进行俩俩拼接
		String[] datas = value.toString().split("\t");
		String friend = datas[0];
		String[] users = datas[1].split(",");
		String sb = new String();
		for (int i = 0; i < users.length; i++) {
			for (int j = 0; j < users.length; j++) {
				if (users[i].compareTo(users[j]) < 0) {
					sb = users[i]+"-"+users[j];
					System.out.println(sb.toString()+friend);
					k.set(sb);
					v.set(friend);
					//发送格式为A-C B 
					//          C-F B等 表示AC CF都关注了B
					            
					context.write(k, v);
				}
			}
		}
	}
}

class Reducer_step02 extends Reducer<Text, Text, Text, Text>{
	Text v = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		//接收格式为A-C B D F  表示AC的共同好友是B D F
		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			sb.append(text.toString()).append(",");
		}
		v.set(sb.substring(0, sb.length()-1).toString());
		context.write(key, v);
	}
}

public class MutiJobs {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(MutiJobs.class);
		
		job1.setMapperClass(Mapper_step01.class);
		job1.setReducerClass(Reducer_step01.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		
		
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(MutiJobs.class);
		
		job2.setMapperClass(Mapper_step02.class);
		job2.setReducerClass(Reducer_step02.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		//conf中设置了文件系统 所以可以直接写文件系统路径
		Path path = new Path("/mutijobout_1_01");
		FileInputFormat.addInputPath(job1, new Path("/mutijobin"));
		FileOutputFormat.setOutputPath(job1, path);
		FileInputFormat.addInputPath(job2, path);
		FileOutputFormat.setOutputPath(job2, new Path("/mutijobout_2_01"));
		
		
		//创建可控制的job
		ControlledJob conjob1 = new ControlledJob(conf);
		ControlledJob conjob2 = new ControlledJob(conf);
		
		//将普通job封装成可控制的job
		conjob1.setJob(job1);
		conjob2.setJob(job2);
		
		//创建job控制器
		JobControl jobcon = new JobControl("my first control job");
		
		//对两个job添加依赖关系
		conjob2.addDependingJob(conjob1);
		
		//将可控制的job加入到job控制器
		jobcon.addJob(conjob1);
		jobcon.addJob(conjob2);
		
		//启动一个线程
		Thread t = new Thread(jobcon);
		t.start();
		while (true) {
			if (jobcon.allFinished()) {
				System.out.println("SUCCESS:"+jobcon.getSuccessfulJobList());    
				jobcon.stop();
				return;
			}
			if(jobcon.getFailedJobList().size() > 0){    
				System.out.println("FAIL:"+jobcon.getFailedJobList());    
				jobcon.stop();    
				return;    
			}   
		}
	
	}
}
