package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * ��ϰ���Ĳ��� 
 * �о����ֶ�ȡ��ʽ 
 * 1>�ֽ���ÿ�ζ�ȡһ���ֽ�
 * 2>ת��Ϊ�ַ��� ÿ�ζ�ȡһ���ַ�
 * 3>ת��ΪBufferedReader ÿ�ζ�ȡһ�� ����һ��string
 * 4>�����е����ݶ�ȡ��һ���ֽ�������
 * 
 * ʹ��write���� ���ַ���ת��Ϊ�ַ����� д���ļ���
 * @author summerKiss
 *
 */

public class ContentHandle {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.159.100:9000"), conf, "hadoop");
		
		FSDataInputStream input = fs.open(new Path("/mr1/mr1"));
		FSDataOutputStream output = fs.create(new Path("/mr1/mr2"));
		
//		//���������ݶ�ȡ��һ���ֽ�������
//		byte[] b = new byte[input.available()];
//		input.read(b);
//		System.out.println(new String(b));
		
//		//ÿ�ζ�ȡһ���ֽ�
//		int a = 0;
//		while ((a = input.read()) != -1) {
//			System.out.println((char)a);
//		}
		
//		//ÿ�ζ�ȡһ���ַ�
//		InputStreamReader inputReader = new InputStreamReader(input);
//		int a = 0;
//		while((a = inputReader.read()) != -1) {
//			System.out.println((char)a);
//		}
		
		//ÿ�ζ�ȡһ��
		InputStreamReader inputReader = new InputStreamReader(input);
		BufferedReader bufferedReader = new BufferedReader(inputReader);
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			String newLine = line.replaceAll("\\s+", "\t") + "\n";
			System.out.println(newLine);
			output.write(newLine.getBytes());
		}
		
		bufferedReader.close();
		output.close();
	}
}