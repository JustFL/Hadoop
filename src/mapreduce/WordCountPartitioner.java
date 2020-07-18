package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * key:map�����key������
 * value:map�����value������
 * */
public class WordCountPartitioner extends Partitioner<Text, IntWritable>{

	
	/**
	 * key:map�����key������
	 * value:map�����value������
	 * numPartitions���������� job.setNumReduceTasks���õ�
	 * �Զ�������ķ���ֵ��Ӧ���ս���ļ��ı��
	 * e.g return 0;��Ӧpart-r-00000
	 * */
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		char c = key.toString().charAt(0);
		if (c >= 'a' && c <= 'g') {
			return 0;
		}else if (c >= 'h' && c <= 'n') {
			return 1;
		}else {
			return 2;
		}
	}
	
}
