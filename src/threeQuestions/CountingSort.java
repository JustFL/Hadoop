package threeQuestions;

/**
 * �������� 
 * ����һ��������� ������±��Ӧԭʼ�����ֵ �����ֵ��Ӧԭʼ����ֵ���ֵĴ���
 * @author summerKiss
 *
 */
public class CountingSort {
	public static void main(String[] args) {
		
		//����һ��20��������ֵ�����  ���м�������
		int[] rands = new int[21];
		for (int i = 0; i < rands.length; i++) {
			
			//����� (��������)(��Сֵ+Math.random()*(���ֵ-��Сֵ+1))
			int rand = (int)(1+Math.random()*(20-1+1));
			rands[rand]++;
		}

		for (int i = 1; i < rands.length; i++) {
			if (rands[i] != 0) {
				for (int j = 1; j <= rands[i]; j++) {
					System.out.print(i+" ");
				}
			}
		}
	}
}
