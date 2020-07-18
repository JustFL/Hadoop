package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * �����ࣺ�����ύ��
 * �������г������jar����������
 * */
public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//�ص�:�����ύ������û� һ��Ҫ����ָ�� 
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		//��ȡ�����ļ�
		Configuration conf = new Configuration();
		//����һ��job ������װmapper��reducer ÿһ������������һ��job
		Job job = Job.getInstance(conf);
		
		//������������ ����ʱҪ���jar������ 
		job.setJarByClass(WordCountDriver.class);
		
		//����mapper��reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//������Ϊ�������ڱ���׶ν��м�� ����ʱ�ͻᱻ���� ���Դ��jar������Ҫ��ָ��һ��
		//����mapper��������� 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//����reducer���������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//�����������·�� args�������г����ʱ�����̨����Ĳ���  ����args[0]�����һ������
		//��������·�� ��Ҫͳ�Ƶ��ʵ�·��
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.159.100:9000/wordcount"));
		//�������·�� ���·�����ܴ��� ����½�ԭ�����ļ�����
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.159.100:9000/wordcountout06"));
	
		/**
		 * ������Ƭ��С ��Ƭ��С����������ݴ洢���С128Mû�й�ϵ  ֻ����Ĭ�ϵ����������ȵ�
		 * maptask����Ĳ��жȾ���ָmaptask����ĸ���
		 * ��Ƭ�ĸ���������maptask����ĸ��� ��Ƭ�ĸ������������е��ļ���С �ļ���������Ƭ��С��ͬ������
		 * ��FileInputFormat���е�getSplits����
		 * long splitSize = computeSplitSize(blockSize, minSize, maxSize);
		 * �� ��Ƭ�Ĵ�С��ȡ�����������м�ֵ�Ĵ�С blockSize��С����Ĭ�ϵ�128M minSize��maxSize��С�������ļ���
		 * "mapreduce.input.fileinputformat.split.maxsize"
		 * "mapreduce.input.fileinputformat.split.minsize"�������ֵ
		 * ��������д�����С�ļ��Ļ� ���Խ���Ƭ��С ��maxsize��С ��֮�����Ҫ������Ƭ��С �ɽ�minSize���
		 * ʵ������в���ֱ���޸������ļ� һ��ֱ���ڴ���������
		 * */
		//��λByte
		//FileInputFormat.setMaxInputSplitSize(job, 330);
		//FileInputFormat.setMinInputSplitSize(job, 20*1024*1024);
		
		//����д���С�ļ� �Ƽ�ʹ��CombineFileInputFormat ���Խ�С�ļ����кϲ���ȡ��һ����Ƭ�� 
		//���´������������ļ���С�����ȻС����Ƭ��С10M ����ֻ����һ����Ƭ
		//job.setInputFormatClass(CombineFileInputFormat.class);
		//CombineFileInputFormat.setMaxInputSplitSize(job, 10*1024*1024);
		
		/**
		 * ����reducer�Ĳ��ж� �����������ٸ�reducetask���� ����job.setNumReduceTasks()����������
		 * reducer�Ĳ��ж�����ʾ�Ͼ����������������ļ����� �����еĽ���ļ��ϲ������������ս��
		 * ���еĽ���ļ����ݶ��ǲ��ظ��� ������ڲ�ʵ���Ǹ���mapreduce��Ĭ��hash������ʽ��ʵ�ֵ�
		 * ���������þ��ǹ滮ÿһ��reducetaskӦ�ü�������ݷ�Χ ������Ʒ�����ʱ��һ��Ҫ�㹻�˽����� ������ܵ��·��������ݲ����� Ҳ����������б
		 * ������Partitioner��Ĭ��ʵ������HashPartitioner
		 * �����Զ���������� ��Ҫע������Զ����������ʱ ���صķ�������Ӧ��С�ڵ���reducetask����ĸ���
		 * ˵���˾��Ǳ�֤ÿһ�����������ݶ���һ��reducetask���������м��� 
		 * ����Զ����������������������reducetask����ĸ��� ���ǵ���ĳ������������û��reducetask������� ���±���
		 * */
		
		//���õ���3��reduretask
		job.setNumReduceTasks(3);
		//ʹ���Զ���ķ�������
		job.setPartitionerClass(WordCountPartitioner.class);
		
		//job�ύ boolean���ʹ����Ƿ��ӡ��־
		job.waitForCompletion(true);
		
		//�������� hadoop jar jar������ �����ȫ·������ ����
		//hadoop jar /home/hadoop/wordcount01.jar mapreduce.WordCountDriver /in /out
		
		
		/**
		 * FileInputFormat �ļ������������� Ĭ��ʹ�õ���ʵ����TextInputFormat
		 * RecordReader    �ļ���ȡ�������� Ĭ��ʹ�õ���ʵ����LineRecordReader �Ὣ�ļ�����ת��Ϊkey-value��ֵ�Ե���ʽ
		 * ����keyΪÿһ�е���ʼƫ����
		 * valueΪÿһ�е�����
		 * 
		 * ��mapper��reducer������ ��shuffle���� ���Զ����ݽ��з��� ���� ����
		 * ����ʹ�õ���WritableComparator
		 * ����ʹ�õ���WritableComparable
		 * ����ʹ��Partitioner��� ��mapper���������з��� Ĭ��ʹ�õ���hashPartitioner
		 * map�˽��кϲ������ Combiner
		 * 
		 * shuffle��˳���������� �ڷ���
		 * 
		 * д����hdfsʱ���Ӧ���ļ�д�������ļ�д�����ο�д���FileOutputFormat/RecordWriter
		 * 
		 * */
		
	}
}
