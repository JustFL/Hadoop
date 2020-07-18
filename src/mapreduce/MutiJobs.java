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
 * ��Job�Ĵ��� ʹ����ͬ������ʾ��
 * ����:
 * A:B,C,D,F,E,O
 * B:A,C,E,K
 * A��ע��B,C,D,F,E,O
 * B��ע��A,C,E,K
 * ��A��B��ͬ��ע����˭
 * ��һ�����ÿһ���˶���˭��ע ����A��B��ע ���Ӧ����A��X,X,X,X��ע
 * �ڶ������õ�һ���Ľ�������е�X��������ƴ�� Ȼ��ת���ǽ��
 */



class Mapper_step01 extends Mapper<LongWritable, Text, Text, Text>{
	
	
	Text k = new Text();
	Text v = new Text();
	/**
	 * ��������Ż����� Mapper����ÿһ��maptask�����Ӧһ�� ����map������ÿһ�����ݵ���һ��  
	 * ���Խ�new Text()�Ķ�������mapper���� ����ᴴ������Ķ���
	 * */
	protected void map(LongWritable key, Text value, Context context) 
			throws java.io.IOException ,InterruptedException {
		//����Ԫ�� �û�A �û���ע�Ķ���B,C,D,F,E,O
		String[] split = value.toString().split(":");
		String person = split[0];
		//����ע�Ķ����б��з�
		String[] friends = split[1].split(",");
		for (String f : friends) {
			k.set(f);
			v.set(person);
			//�����B:A C:A D:A F:A ... ���з���
			context.write(k, v);
		}
	}
}

class Reducer_step01 extends Reducer<Text, Text, Text, Text>{
	Text v = new Text();
	
	//���ܵ�Ԫ�� �û�B ��A��һϵ�ж����ע ����һϵ�ж����ó��� ����,ƴ��
	protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) 
			throws java.io.IOException ,InterruptedException {
		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			sb.append(text.toString()+",");
		}
		
		v.set(sb.substring(0, sb.length()-1));
		//������ʽ B A,C,F��
		context.write(key, v);
	}
}


class Mapper_step02 extends Mapper<LongWritable, Text, Text, Text>{
	Text k = new Text();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//���ݸ�ʽB A,C,F �Թ�עB��һϵ���û� ��������ƴ��
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
					//���͸�ʽΪA-C B 
					//          C-F B�� ��ʾAC CF����ע��B
					            
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
		
		//���ո�ʽΪA-C B D F  ��ʾAC�Ĺ�ͬ������B D F
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
		
		//conf���������ļ�ϵͳ ���Կ���ֱ��д�ļ�ϵͳ·��
		Path path = new Path("/mutijobout_1_01");
		FileInputFormat.addInputPath(job1, new Path("/mutijobin"));
		FileOutputFormat.setOutputPath(job1, path);
		FileInputFormat.addInputPath(job2, path);
		FileOutputFormat.setOutputPath(job2, new Path("/mutijobout_2_01"));
		
		
		//�����ɿ��Ƶ�job
		ControlledJob conjob1 = new ControlledJob(conf);
		ControlledJob conjob2 = new ControlledJob(conf);
		
		//����ͨjob��װ�ɿɿ��Ƶ�job
		conjob1.setJob(job1);
		conjob2.setJob(job2);
		
		//����job������
		JobControl jobcon = new JobControl("my first control job");
		
		//������job���������ϵ
		conjob2.addDependingJob(conjob1);
		
		//���ɿ��Ƶ�job���뵽job������
		jobcon.addJob(conjob1);
		jobcon.addJob(conjob2);
		
		//����һ���߳�
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
