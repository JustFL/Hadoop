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
 * ԭʼ����
 * 1        0        �ҵ�
 * 2        0        ��װ
 * 3        0        ʳƷ
 * 4        1        ϴ�»�
 * 5        1        ����
 * 10       4        ����
 * 11       5        ����
 * 
 * �������
 * �ҵ�        ϴ�»�-����    
 * �ҵ�        ����-����   
 * @author summerKiss
 *
 */

class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	Text k = new Text();
	Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String[] split = value.toString().split("\t");
		//�Ƕ�������
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
		 * ����ΪʲôҪ��ô��
		 * ���ȷ��͹�����������������
		 * 1        0		�ҵ�
		 * 1        4		ϴ�»�
		 * 1        5		����
		 * 
		 * keyֵ����ʲô ��Ҫ��values�Ŀ�ʼ�ַ�ȷ�� 
		 * ��0��ͷ��ʾ�Ƕ������� keyֵ�Ƕ��������id
		 * ����0��ͷ�� keyֵ������id 
		 * ��keyֵ��ͬ��Ϊһ�� ���͹�������������ʵ�и���Ҳ���ӷ��� ��Ҫ����ƴ��
		 * 
		 * ����Ҫ����������       
		 * 4        0		�ҵ�-ϴ�»�
		 * 5        0		�ҵ�-����
		 * ƴ�������Ҫ����0 ��ʾ�Ƕ������� �Ա����ѭ��
		 * 
		 * �����ǻ���һЩ���ݲ��Ǹ���������ϵ� 
		 * e.g
		 * 4        10      ����
		 * ���������ʵ�Ǽҵ�������� �ǵڶ������ܽ���ƴ�ӵ� ������ݵķ�����û�и��� ��Ҫ��������
		 * 
		 * ���⻹��һ��
		 * 9        3        ˮ��
		 * ��������ڵ�һ������ƴ����ɺ� ���γ�������
		 * 9		0        ʳƷ-ˮ��
		 * ���������û������� ֻ�и��� Ҳ��Ҫ��������
		 *
		 * �������Ϸ������������ ��ÿһ�������ʱ�� ������Ҫ���Ƚ��б��� �鿴���и��������Ĵ������ ��ΪIterable<Text>���ֻ�ܱ���һ��
		 * ���Խ������������ݴ洢 ���һ����������������Ƿ���ڸ�������ж�
		 */
		String father = "";
		ArrayList<String> children = new ArrayList<String>();
		for (Text t : values) {
			String value = t.toString();
			//�ҵ����� 
			if (value.startsWith("0")) {
				father = value;
			}else {
				children.add(value);
			}
		}
		/**
		 * ���û�и��� ����map��ת�����ݻ�ԭ ׼��������һ��map
		 * 4        10      ����
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
			 * ���ֻ�и���
			 * 9		0        ʳƷ-ˮ��
			 * ֱ�����
			 */
			
			k.set(key);
			v.set(father);
			context.write(k, v);
			
		}else {
			/**
			 * ������� ���и������ֺ��������ֵ�ƴ��
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
