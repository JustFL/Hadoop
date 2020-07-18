package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * ʹ��������hdfs���ϴ����� ������������ָ�����ڴ� �����ϴ����� ����ָ���Ǵӱ������뵽�ڴ��� ���ָ���Ǵ��ڴ������hdfs
 * @author summerKiss
 *
 */

public class HdfsStream {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.159.100:9000"), conf, "hadoop");
		
		//����о��˳�
		Path path = new Path("/HdfsStream.java");
		if (fs.exists(path)) {
			return;
		}
		
		//�����ļ�ϵͳ������ ʹ�������·��
		FileInputStream in = new FileInputStream(new File("src/hdfs/HdfsStream.java"));
		//hdfs�ļ�ϵͳ����� ����ָ���ļ���
		FSDataOutputStream out = fs.create(new Path("/HdfsStream.java"));
		//�ϴ��ļ�
		IOUtils.copyBytes(in, out, 4096);
		
		//hdfs�ļ�ϵͳ������
		FSDataInputStream in1 = fs.open(new Path("/HdfsStream.java"));
		//�����ļ�ϵͳ�����
		FileOutputStream out1 = new FileOutputStream(new File("D:\\HdfsStream.java"));
		
		/**
		 * ���������ж�ȡ����ʼλ��
		 * public void seek(long desired);
		 * ���ö�ȡ����
		 * public static void copyBytes(InputStream in, OutputStream out, long count, boolean close);
		 */
		
		//�����ļ�
		IOUtils.copyBytes(in1, out1, 4096);
		
		fs.close();
	}
}
