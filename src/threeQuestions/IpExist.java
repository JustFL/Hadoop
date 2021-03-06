package threeQuestions;
/**
 * 
 * @author summerKiss
 * 快速判断给定的ip地址在文件中是否存在
 * BF是由一个长度为m比特的位数组（bit array）与k个哈希函数（hash function）组成的数据结构。
 * 位数组均初始化为0，所有哈希函数都可以分别把输入数据尽量均匀地散列。
 * 当要插入一个元素时，将其数据分别输入k个哈希函数，产生k个哈希值。以哈希值作为位数组中的下标，将所有k个对应的比特置为1。
 * 当要查询（即判断是否存在）一个元素时，同样将其数据输入哈希函数，然后检查对应的k个比特。如果有任意一个比特为0，表明该元素一定不在集合中。
 * 如果所有比特均为1，表明该集合有（较大的）可能性在集合中。
 * 为什么不是一定在集合中呢？因为一个比特被置为1有可能会受到其他元素的影响，这就是所谓“假阳性”（false positive）。
 * 相对地，“假阴性”（false negative）在BF中是绝不会出现的。
 */

public class IpExist {
	public static void main(String[] args) {
		
		
	}
}
