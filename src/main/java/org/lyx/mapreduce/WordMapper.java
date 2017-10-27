package org.lyx.mapreduce;
/**
 * 版权所有：厦门欣汇峰贸易有限公司
 *====================================================
 * 文件名称: WordMapper.java
 * 修订记录：
 * No    日期				作者(操作:具体内容)
 * 1.    2017年4月23日			liuyuanxian(创建:创建文件)
 *====================================================
 * 类描述：(说明未实现或其它不应生成javadoc的内容)
 * 
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
 
public class WordMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
 
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
    	final IntWritable ONE = new IntWritable(1);
    	//接收到split中的输出结果作为map的输入
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word : words) {
            //放到list中，每个text统计加1，作为map的输出
            //写到context上下文环境中，输出到下一个执行过程shuffle
            context.write(new Text(word), ONE);
        }
    }
     
}
