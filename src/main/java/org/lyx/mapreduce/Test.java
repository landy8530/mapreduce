package org.lyx.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 版权所有：厦门欣汇峰贸易有限公司
 *====================================================
 * 文件名称: Test.java
 * 修订记录：
 * No    日期				作者(操作:具体内容)
 * 1.    2017年4月23日			liuyuanxian(创建:创建文件)
 *====================================================
 * 类描述：(说明未实现或其它不应生成javadoc的内容)
 * 
 */
public class Test {

	public static void main(String[] args) throws Exception {
	    // true/false：读不读配置文件
//        Configuration conf = new Configuration(true);

		Configuration conf = new Configuration();
        //3.直接在远程服务器运行
        //配置了fs.defaultFS选项，则下面的setInputPaths等配置不需要再配置前缀
		conf.set("fs.defaultFS", "hdfs://192.168.56.200:9000/");
        //配置此工程打好的jar包路径，建议使用相对路径
		//conf.set("mapreduce.job.jar", "target/mapreduce-0.0.1-SNAPSHOT.jar");
		conf.set("mapreduce.framework.name", "yarn");//MapReduce运行环境为yarn
		conf.set("yarn.resourcemanager.hostname", "192.168.56.200");//yarn的recourseManager的主机名
		conf.set("mapreduce.app-submission.cross-platform", "true");//表示MapReduce是跨平台运行的
		
        Job job = Job.getInstance(conf);
        //
        job.setJar("target/mapreduce-0.0.1-SNAPSHOT.jar");

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        //mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //reducer输出类型，如果mapper和reducer输出类型一样，只要设置reducer输出类型就行
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //1.直接本地输入输出
//        FileInputFormat.setInputPaths(job, "D:/bigdata/test/test.txt");
//        FileOutputFormat.setOutputPath(job, new Path("D:/bigdata/test/out4/"));
        //2.把hdfs中的文件拉到本地来运行
//        FileInputFormat.setInputPaths(job, "hdfs://192.168.56.200:9000/wcinput/");
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.56.200:9000/wcoutput/"));
        //3.直接在远程服务器运行
        FileInputFormat.setInputPaths(job, "/wcinput/");
        Path output =  new Path("/wcoutput/");
        //存在就先删除输出目录
        if (output.getFileSystem(conf).exists(output)) {
            output.getFileSystem(conf).delete(output, true);
        }

        FileOutputFormat.setOutputPath(job, output);
         
        job.waitForCompletion(true);

	}

}
