package threeQuestions;

/**
 * 求两个文件中相同的IP
 * 
 * 如果是两个非常大的大文件 还是要采取分而治之的思想 假如两个文件都分为4个小文件 编号1-4
 * 但是直接分开 则需要互相比较每个文件 需要比较16次 
 * 需要让对应编号文件内的ip范围相同 可以对每个ip求hashcode() 然后对4取余 保证每个文件内的ip范围都是一定的
 * 这样编号相对应的4个文件进行4次比较就可以取出来相同的ip 最后再进行汇总就可以
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
