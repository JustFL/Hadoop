package hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsBase {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		/**
		 * Configuration����������ȡ�����ļ�:
		 * 1>core-default.xml
		 * 2>hdfs-default.xml
		 * 3>mapred-default.xml
		 * 4>yarn-default.xml
		 * hdfs����������ʱ
		 * ��һ��>Ĭ�϶�ȡ����jar���е������ļ�
		 * �ڶ���>��ȡ��Ŀ��classpath(src)�µ������ļ�(���Ŀ¼�µ������ļ�����ֻ����hdfs-site.xml����hdfs-default.xml ���������޷�ʶ��) 
		 * ����ǿ�Ƽ��ر������conf.addResource("XXX");
		 * ������>��ȡ������ָ�������ļ������Ե�ֵ
		 * conf.set("dfs.replication", "5");
		 * */
		
		Configuration conf = new Configuration();
		
		/**
		 * ָ�������ļ�ϵͳ���û������ַ�ʽ
		 * 1>��ʼ��FileSystem����ʱ ָ���û�
		 * FileSystem fs = FileSystem.get(new URI("hdfs://192.168.121.10:9000"), conf, "hadoop");
		 * 2>ʹ��ϵͳ��System�������û� һ��Ҫ���ڴ�����ǰ��
		 * System.setProperty("HADOOP_USER_NAME", "hadoop");
		 * 3>����ʱָ��run as > run configurations > Arguments > VM arguments > -DHADOOP_USER_NAME=hadoop
		 * */
		
		//����Ŀ¼������ ���������URI �򴴽������ļ�ϵͳ ����ָ���Ǵ������еĵط�
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.159.100:9000"), conf);
		
		//����Ŀ¼ ���Լ�������
		if (!fs.exists(new Path("/hdfsbase"))) {
			fs.mkdirs(new Path("/hdfsbase"));
			
			Path src = new Path("config/ipCount.txt");
			Path dst = new Path("/hdfsbase");
			
			fs.copyFromLocalFile(src, dst);
		}
	
		/**
		 * namenode��VERSION�ļ����ݽ���
		 * 		namespaceID=1009554514
				clusterID=CID-5253b8a5-c3f0-4549-a445-3c17e7dde8fa --��Ⱥ��ʶ
				cTime=0
				storageType=NAME_NODE
				blockpoolID=BP-372562848-192.168.121.10-1561277050411 --���ID ����ģʽ�� ��ͬ��namenode����Ŀ��ID��ͬ
				layoutVersion=-63
		 * */
		
		/**
		 * �ļ��ϴ���ɺ� �����Ĵ洢Ŀ¼����datanode�� ����Ŀ¼Ϊ /home/hadoop/data/hadoopdata/data��
		 * current�洢��ʵ������
		 * in_use.lock�����ļ� ��ʶdatanode���� һ���ڵ�ֻ�ܿ���һ��datanode����
		 * currentĿ¼����һ����namenode��VERSION�ļ��е�blockpoolIDΪ���ֵ�Ŀ¼ ���еĿ���Ϣ�������Ŀ¼��
		 * ���յĴ洢Ŀ¼/home/hadoop/data/hadoopdata/data/current/BP-372562848-192.168.121.10-1561277050411/current/finalized/subdir0/subdir0
		 * ÿ���ļ����ϴ������л����������ļ�
		 * blk_1073741832_1008.meta ԭʼ�ļ���Ԫ������Ϣ  ���ڼ�¼ԭʼ�ļ��ĳ��� ����ʱ�� ƫ��������Ϣ
		 * blk_1073741832  ԭʼ�ļ� �����������Block ID ȫ��Ψһ
		 * .crc�ļ������ص�ʱ�����ɵ� ����У���ļ��������� У������ļ�����ʼƫ�����ͽ�βƫ���� ����м�����ݲ������ı���У��ͨ��
		 * ����м����ݷ����ı� ��Checksum error
		 * */

		//��ȡһ��Ŀ¼�µ������ļ��ľ�����Ϣ(�����ļ��Ŀ�ľ�����Ϣ) ���ܻ�ȡĿ¼
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), false);
		while (listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			//����ÿ���ļ��Ŀ���Ϣ
			BlockLocation[] blockLocations = next.getBlockLocations();
			System.out.println(blockLocations.length);
			for (BlockLocation blockLocation : blockLocations) {
				//�����������ÿ���ļ���ÿһ�������Ϣ ������ʼƫ���� ��βƫ�����������洢���ĸ��ڵ���
				System.out.println(blockLocation);
			}
		}
		
		System.out.println("~~~~~~~~~~~~~");
		//��������г�ָ���ļ����µ������ļ������ļ���  ����ֻ����ʾһЩ������Ϣ
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			System.out.println(fileStatus);
		}
		
		
		fs.close();

	} 
}
