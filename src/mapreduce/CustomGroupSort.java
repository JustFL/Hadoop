package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ͬʱ�����Զ����������� 
 * �����п�Ŀ��ƽ������ߵ�ѧ�� ��� course name grade
 * @author summerKiss
 *
 */
class ScoreBean implements WritableComparable<ScoreBean>{
	
	private String course;
	private double grade;
	
	public String getCourse() {
		return course;
	}
	public void setCourse(String course) {
		this.course = course;
	}
	public double getGrade() {
		return grade;
	}
	public void setGrade(double grade) {
		this.grade = grade;
	}
	
	public ScoreBean(String course, double grade) {
		super();
		this.course = course;
		this.grade = grade;
	}
	
	public ScoreBean() {
		super();
	}
	
	@Override
	public String toString() {
		return "CustomGroupSort [course=" + course + ", grade=" + grade + "]";
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(course);
		out.writeDouble(grade);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.course = in.readUTF();
		this.grade = in.readDouble();
	}
	
	@Override
	public int compareTo(ScoreBean o) {
		/**
		 * double�����������double Ҫ�󷵻�int ������Ҫ�ж�
		 * ��Ϊ���ǵ�������Ҫ�Ƚ��пγ̷��� �ٽ��з������� �����Ĭ������������ 
		 * ��������ʱ �Ƚ��γ����� ����һ���Ŀγ̿϶�����һ���� Ȼ���ٽ���������
		 * 
		 * ����������������ʱ ��Ҫ�������ֶ���������Χ ��������ʱ ���ŷ����ֶ� �ٰ��������ֶ�����
		 */
		
		if (o.getCourse().compareTo(this.course) == 0) {
			return o.getGrade() - this.getGrade() > 0 ? 1 : (o.getGrade() - this.getGrade() < 0 ? -1 : 0);
		}else {
			return o.getCourse().compareTo(this.course);
		}
	}
}

class GroupSortMapper extends Mapper<LongWritable, Text, ScoreBean, Text>{
	
	ScoreBean sb = new ScoreBean();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		String course = split[0].trim();
		String name = split[1].trim();
		int sum = 0;
		int count = 0;
		for (int i = 2; i < split.length; i++) {
			count++;
			sum+=Integer.parseInt(split[i]);
		}
		double avg = (double)sum / count;
		sb.setCourse(course);
		sb.setGrade(avg);
		v.set(name);
		context.write(sb, v);
	}
}

class GroupSortReducer extends Reducer<ScoreBean, Text, Text, NullWritable>{
	Text k = new Text();
	@Override
	protected void reduce(ScoreBean key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
//		System.out.println("=============================");
//		for (Text text : values) {
//			System.out.println(key.getCourse()+"\t"+text.toString()+"\t"+key.getGrade());
//		}
		
		
		//����Ĭ��key���Ѿ���ɷ�������� ֻ��Ҫȡ��һ��ֵ����
		Text name = values.iterator().next();
		k.set(key.getCourse()+"\t"+name+"\t"+key.getGrade());
		context.write(k, NullWritable.get());
	}
}

class MyGroup extends WritableComparator{
	//Ĭ������µڶ�������Ϊfalse   ���ṹ��ʵ������  ����������Ҫ�ֶ�����һ��  ����true
	public MyGroup() {
		super(ScoreBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		/**
		 * �������ֻ�Ƚ�ǰһ�����ݺͺ�һ������ �����ͬ����Ϊһ�� ��ͬ�ͻ����»���Ϊһ�� ���Ա����Ƚ�������
		 */
		ScoreBean asb=(ScoreBean)a;
		ScoreBean bsb=(ScoreBean)b;
		//ֻ���ķ���0��ֵ
		return asb.getCourse().compareTo(bsb.getCourse());
	}
}

public class CustomGroupSort {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		conf.addResource("config/core-site.xml");
		conf.addResource("config/hdfs-site.xml");
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CustomGroupSort.class);
		
		job.setGroupingComparatorClass(MyGroup.class);
		
		job.setMapperClass(GroupSortMapper.class);
		job.setReducerClass(GroupSortReducer.class);
		
		job.setMapOutputKeyClass(ScoreBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/score"));
		FileOutputFormat.setOutputPath(job, new Path("/customgroupsort_03"));
		
		job.waitForCompletion(true);
		
	}
}
