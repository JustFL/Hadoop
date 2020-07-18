package hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * ɾ��HDFS��Ⱥ�е����п��ļ��Ϳ�Ŀ¼
 * */
public class DeleteAllEmpty {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.159.100:9000"), conf, "hadoop");
		
		//Upload(fs, "D:\\BaiduNetdiskDownload\\muti_job.txt", "/mutijobin");
		DeleteAll(fs, "/");
	}

	public static void DeleteAll(FileSystem fs, String path) throws FileNotFoundException, IOException {
		Path p = new Path(path);
		FileStatus[] filestatus = fs.listStatus(p); 
		if (filestatus.length == 0) {
			fs.delete(p, false);
		}else {
			for (FileStatus f : filestatus) {
				if (f.isDirectory()) {
					DeleteAll(fs, f.getPath().toString());
				}else {
					if (f.getLen() == 0) {
						fs.delete(f.getPath(), false);
					}
				}
			}
			
			//ɾ�����ļ����ļ��к� �ٴμ���Ƿ�Ϊ��
			FileStatus[] afterdelete = fs.listStatus(p); 
			if (afterdelete.length == 0) {
				fs.delete(p, false);
			}
		}
		
	}
	
	
}
