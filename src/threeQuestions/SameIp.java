package threeQuestions;

/**
 * �������ļ�����ͬ��IP
 * 
 * ����������ǳ���Ĵ��ļ� ����Ҫ��ȡ�ֶ���֮��˼�� ���������ļ�����Ϊ4��С�ļ� ���1-4
 * ����ֱ�ӷֿ� ����Ҫ����Ƚ�ÿ���ļ� ��Ҫ�Ƚ�16�� 
 * ��Ҫ�ö�Ӧ����ļ��ڵ�ip��Χ��ͬ ���Զ�ÿ��ip��hashcode() Ȼ���4ȡ�� ��֤ÿ���ļ��ڵ�ip��Χ����һ����
 * ����������Ӧ��4���ļ�����4�αȽϾͿ���ȡ������ͬ��ip ����ٽ��л��ܾͿ���
 * 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class SameIp {
	public static void main(String[] args) throws IOException {
		File f1 = new File("config/ipCount.txt");
		FileReader fr1 = new FileReader(f1);
		BufferedReader br1 = new BufferedReader(fr1);
		
		HashSet<String> set1 = new HashSet<String>();
		String str1;
		while ((str1 = br1.readLine()) != null) {
			set1.add(str1);
		}
		br1.close();
		
		
		
		File f2 = new File("config/sameIp.txt");
		FileReader fr2 = new FileReader(f2);
		BufferedReader br2 = new BufferedReader(fr2);
		
		HashSet<String> set2 = new HashSet<String>();
		String str2;
		while ((str2 = br2.readLine()) != null) {
			set2.add(str2);
		}
		br2.close();
		
		HashSet<String> result = new HashSet<String>();
		for (String string : set2) {
			if (set1.contains(string)) {
				result.add(string);
			}
		}
		
		System.out.println(result);
	}
}
