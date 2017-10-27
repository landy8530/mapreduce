package org.lyx.mapreduce;
/**
 * 版权所有：厦门欣汇峰贸易有限公司
 *====================================================
 * 文件名称: WordReducer.java
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
import org.apache.hadoop.mapreduce.Reducer;
 
public class WordReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
 
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long count = 0;
        //此时的输入是shuffle的输出
        //输入的key是字符串，输入的value是shuffle派发过来的是一个个list
        for(IntWritable v : values) {
            count += v.get();
        }
        context.write(key, new LongWritable(count));
    }
     
}
