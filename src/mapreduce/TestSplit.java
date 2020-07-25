package mapreduce;

import java.util.Arrays;
/**
 * split() ��������ƥ�������������ʽ������ַ���
 * ע�⣺ . $ |  * ��ת���ַ� ����ü� \\
 * ע�⣺����ָ��� ������ | ��Ϊ���ַ���
 * �﷨
 * 	public String[] split(String regex, int limit)
 * ����
 * 	regex -- ������ʽ�ָ�����
 * 	limit -- �ָ�ķ�����
 * @author summerKiss
 *
 */

public class TestSplit {
	public static void main(String[] args) {
        String str = new String("Welcome-to-Runoob");
        
        System.out.println("- �ָ�������ֵ :" );
        for (String retval: str.split("-")){
            System.out.println(retval);
        }
 
        System.out.println("");
        System.out.println("- �ָ������÷ָ��������ֵ :" );
        for (String retval: str.split("-", 2)){
            System.out.println(retval);
        }
 
        System.out.println("");
        String str2 = new String("www.runoob.com");
        System.out.println("ת���ַ�����ֵ :" );
        for (String retval: str2.split("\\.", 3)){
            System.out.println(retval);
        }
 
        System.out.println("");
        String str3 = new String("acount=? and uu =? or n=?");
        System.out.println("����ָ�������ֵ :" );
        for (String retval: str3.split("and|or")){
            System.out.println(retval);
        }
        
		//����ָ�������ֵ
        System.out.println("");
        String tempAuthorStr="����;����,�����أ����壻�ܲ� ���";;
        String[] tmpAuthors=tempAuthorStr.split("\\s+|;|,|��|��");
        System.out.println(Arrays.toString(tmpAuthors));
       
        
	}
}
