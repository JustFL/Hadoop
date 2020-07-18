package hdfs;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ������ ɾ��ʵ�����ɵ�Ŀ¼
 * @author summerKiss
 *
 */
public class DeleteSpecificPath {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fStatus : listStatus) {
			if (fStatus.isDirectory() && fStatus.getPath().getName().contains("groupsort")) {
				fs.delete(fStatus.getPath(), true);
			}
		}
	}
}
