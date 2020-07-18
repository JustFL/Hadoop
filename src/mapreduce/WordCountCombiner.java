package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * �Զ����齨combiner Ĭ�������û�������� ������map��reduce֮���һ������
 * �����Ƿֵ�reducertask��ѹ�� ��Ϊreducertask���̵Ĳ��жȲ��� ��maptask���̵Ĳ��жȿ��Ժܸ� 
 * maptask�Ĳ��жȺ���Ƭֱ�ӹҹ�  ���������������� ��Ƭ�����кܶ� ����maptask�Ĳ��жȿ��Ժܸ�
 * combiner��������þ�����map�˽�����һ�κϲ� ������shuffle���̵������� ����˳���Ч��
 * combiner��ҵ���߼���reducer��һ��һ�� ����ֱ���ճ�reduce�Ĵ��� ��������ֱ��
 * job.setCombinerClass(WordCountReducer.class);��reducer��Ϊcombiner���ֱ��ʹ��
 * �䱾�ʾ�����map�˽�����һ��reduce
 * 
 * ע��! combiner����������� ���ֵ ��Сֵ ���ǲ���������ƽ��ֵ
 * */

/**
 * ����combiner��map��reduce֮���һ������ ����ǰ����������map����� ������������reduce������
 * ��Ϊmap���������reduce������ ����ǰ�����ͺ�������������ȫһ�µ�
 * 
 * */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	/**
	 * ���reduce������Ӧ����һ����Ƭ Ҳ����һ��maptask ����һ����Ƭ�е����� �����ԶԶ��maptask�Ľ�����кϲ�
	 * */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable i : values) {
			sum += i.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
