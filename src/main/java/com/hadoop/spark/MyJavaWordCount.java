package com.hadoop.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MyJavaWordCount {

	public static void main(String[] args) {
		// 检测参数
		if(args.length<2){
			System.out.println("输入正确的参数");
			System.exit(1);
		}
		String input = args[0];
		String output = args[1];
		
		//创建Java版的SparkContext
		SparkConf conf = new SparkConf().setAppName("MyJavaWordCount");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//读取数据
		JavaRDD<String> inputRdd = sc.textFile(input);
		//进行相关计算
		JavaRDD<String> words = inputRdd.flatMap(new FlatMapFunction<String,String>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line)throws Exception{
				return  Arrays.asList(line.split("\t")).iterator();
			}
		});
		JavaPairRDD<String, Integer> result = words.mapToPair(new PairFunction<String,String,Integer>(){
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String,Integer> call(String word) throws Exception{
				return new Tuple2<String, Integer>(word,1);
			}
		}).reduceByKey(new Function2<Integer,Integer,Integer>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x,Integer y)throws Exception{
				return x+y;
			}
		});
		result.saveAsTextFile(output);
		sc.stop();
	}
}
