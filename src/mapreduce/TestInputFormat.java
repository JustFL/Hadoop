package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * @author zy
 * �Զ��������� ʵ�ֽ�һ���ļ���������Ϊkey
 */
public class TestInputFormat {
	
	/**
	 * �Զ���������̳�FileInputFormat 
	 * ���������Լ����� ����Ҫ��ȡ�����ļ� �������ļ������ݶ�ȡ��key�� value��NullWritable����
	 * */
	static class MyInputFormat extends FileInputFormat<Text, NullWritable>{

		@Override
		public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			//���ｫ��Ƭ�������Ķ���ͨ��initialize���������ļ���ȡ����  ������ȡ·���ʹ���FileSystem����
			MyRecordReader record = new MyRecordReader();
			record.initialize(split, context);
			return record;
		}
	}
	
	static class MyRecordReader extends RecordReader<Text, NullWritable>{
		
		//�ļ�������
		FSDataInputStream open = null;
		//��¼�ļ�����
		long len= 0;
		//����ļ�֪���ȡ���
		boolean isFinish = false;
		//keyΪText���� ���д洢�ļ�����
		Text k = new Text();
		
		/**
		 * ����������г�ʼ�� Ҫ�����ļ���ȡ Ҫ����һ��������
		 * ��������������Ҫ����Ҫ��ȡ�ļ��ĵ�ַ �ļ���ַ���Դ�split��Ƭ�ϻ�ȡ
		 * */
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			//����FileSystem����
			FileSystem fs = FileSystem.get(context.getConfiguration());
			//�����ļ�·������ ��ΪInputSplit�ǳ����� ������ǿתΪ��ʵ����
			FileSplit s = (FileSplit)split;
			//��ȡ·��
			Path path = s.getPath();
			//Ϊ������ֵ
			open = fs.open(path);
			//��ȡ�ļ�����
			len = split.getLength();
		}

		/**
		 * �����ļ���ȡ ����һ��booleanֵ��ʾ�ļ��Ƿ��ȡ����
		 * */
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			
			if (!isFinish) {
				//����һ���ֽ��������洢�ļ����� �ļ����ȸ���split��ȡ
				byte[] b = new byte[(int)len];
				//���ļ����ݶ�ȡ��������
				open.readFully(0, b);
				//���ַ�����洢��key-value��ֵ�Ե�key��
				k.set(b);
				isFinish = true;
				return isFinish;
			}else {
				isFinish = false;
				return isFinish;
			}
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			
			return k;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException, InterruptedException {
			
			return NullWritable.get();
		}

		/**
		 * �ļ���ȡ�Ľ���
		 * */
		@Override
		public float getProgress() throws IOException, InterruptedException {
			//����򵥵ķ���һ�� ���귵��1.0���򷵻�0.0
			return isFinish?1.0F:0.0F;
		}

		@Override
		public void close() throws IOException {
			
			open.close();
			
		}
		
	}
	
	/**
	 * �����Զ����������� �����������������Ҫ�����Զ����������
	 * */
	static class MyMapper extends Mapper<Text, NullWritable, Text, NullWritable>{
		@Override
		protected void map(Text key, NullWritable value, Context context)
				throws IOException, InterruptedException {
			//��Ϊ�������ļ���ȡ ����ֱ�ӷ���
			context.write(key, NullWritable.get());
		}
	}
	
	static class MyReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			//map�������ļ����� �м�shuffle���̰����ļ����ݷ��� ������ȫһ�µķֳ�һ��
			context.write(key, NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.199.10:9000");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(TestInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//����ʹ���Զ�����������
		job.setInputFormatClass(MyInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/inputformat"));
		FileOutputFormat.setOutputPath(job, new Path("/inputout08"));
		
		job.waitForCompletion(true);
	}
}
