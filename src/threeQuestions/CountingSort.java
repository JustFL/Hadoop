package threeQuestions;

/**
 * 计数排序 
 * 创建一个结果数组 数组的下标对应原始数组的值 数组的值对应原始数组值出现的次数
 * @author summerKiss
 *
 */
public class CountingSort {
	public static void main(String[] args) {
		
		//产生一个20个随机数字的数组  进行计数排序
		int[] rands = new int[21];
		for (int i = 0; i < rands.length; i++) {
			
			//随机数 (数据类型)(最小值+Math.random()*(最大值-最小值+1))
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
