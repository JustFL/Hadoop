package mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * KEYIN:������ļ���ÿ�е���ʼƫ���� Long���� mapreduce�ײ�Ҳ�������������� ʹ�õ����ֽ���
 * VALUEIN:�����ÿ�е����� String����
 * KEYOUT:���� String����
 * VALUEOUT:��ǩ 1 int����
 * ���������ڽ������ݴ����ʱ�����Ҫ�������紫��ʹ��̳־û� �������ݱ���Ҫ�������л�
 * ����ʹ��hadoop�Դ������л��ӿ�writable
 *  
 * */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		/**
		 * map��������������
		 * LongWritable keyÿ�е�ƫ���� 
		 * Text valueÿ�е�����
		 * Context context�����Ķ��� ���ڴ���
		 * ��������ĵ���Ƶ����ÿ�е���һ��
		 * ����Ĵ���˼·�� ÿ�е���һ�ε�ʱ�� ��ÿ�е����ݽ����з� ���Ҷ�ÿ�����ʽ��д��ǩ1
		 * */
		//ÿ�е����� value��Text���� ��Ҫ�����л�
		String line = value.toString().trim();
		//��ÿ�������зֳ����� ע��split�����������������ʽ �����ǵ������ַ��� ���������ת���ַ� Ҫʹ��\\
		//����μ�TestSplit
		String[] words = line.split("\\s+|;|,|\\.|!|��");
		
		Pattern p = Pattern.compile("\\w+");
		//�������� ֱ�ӷ��͵�reduce��
		for (String word : words) {
			Matcher m = p.matcher(word);
			boolean matches = m.matches();
			if (matches) {
				System.out.println(word);
				Text out_word = new Text(word);
				IntWritable out_tag = new IntWritable(1);
				context.write(out_word, out_tag);
			}
			
		}
	}
}
