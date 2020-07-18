package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer���������mapper�����
 * KEYIN ��mapper����ĵ��� ����Text
 * VALUEIN ��mapper����ı�ǩ1 ����IntWritable
 * KEYOUT �����վ���ͳ�Ƶĵ��� ����Text
 * VALUEOUT �����վ���ͳ�Ƶĵ��ʳ��ֵĴ��� ����IntWritable
 * 
 * */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		/**
		 * �ڴ�mapper��reducer�Ĺ����� ��ܶ����ݽ����˷��鴦�� ����mapper�������key��ֵ�����˷���
		 * keyֵ��ͬ��Ϊһ�� �ж��ٸ���ͬ��key���ж�����
		 * Text arg0 :ÿ�����Ǹ���ͬ��key
		 * Iterable<IntWritable> arg1:ÿһ������ͬ��key����Ӧ��ȫ����valueֵ
		 * Context arg2 :����д�� ֱ��д��hdfs
		 * 
		 * ��������ĵ���Ƶ��Ϊһ�����һ��
		 * 
		 * */
		int sum = 0;
		for (IntWritable i : arg1) {
			sum += i.get();
		}
		arg2.write(arg0, new IntWritable(sum));
	}
}
