package threeQuestions;
/**
 * ��һ���ļ��г��ִ��������Ǹ�IP
 * 
 * �����һ���ǳ�����ļ� ˼·�� �����ļ��ָ��ΪС�ļ� ��С�ļ�����ͳ�� �����л���
 * */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class IpCount {
	public static void main(String[] args) throws IOException {
		File f = new File("config/ipCount.txt");
		FileReader fr = new FileReader(f);
		BufferedReader br = new BufferedReader(fr);
		
		HashMap<String, Integer> record = new HashMap<String, Integer>(); 
		
		String str;
		while ((str = br.readLine()) != null) {
			if (record.containsKey(str)) {
				int value = record.get(str);
				record.replace(str, value+1);
			}
			else {
				record.put(str, 1);
			}
		}
		
		int max = 0;
		String maxip = "";
		for (Entry<String, Integer> entry : record.entrySet()) {
//			System.out.println("<"+entry.getKey()+":"+entry.getValue()+">");
			if (entry.getValue() > max) {
				max = entry.getValue();
				maxip = entry.getKey();
			}
		}
		
		System.out.println("��������IP��:"+maxip);
		br.close();
	}
}
