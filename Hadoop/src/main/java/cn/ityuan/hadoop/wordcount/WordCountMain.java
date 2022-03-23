package cn.ityuan.hadoop.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Classname WordCountMain
 * @Description TODO
 * @Date 2022/3/23 21:38
 * @Created by liudan
 */
public class WordCountMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"WordCount");

        job.setJarByClass(WordCountMain.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Path input = new Path("Hadoop/data/input/words.txt");
        Path output = new Path("Hadoop/data/output/wordcount/");

        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 :1);
    }

    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        private static final LongWritable ONE = new LongWritable(1L);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokens = new StringTokenizer(value.toString()," ");
            while (tokens.hasMoreTokens()) {
                String token = tokens.nextToken();
                if (!StringUtils.isBlank(token)) {
                    word.set(token);
                    context.write(word, ONE);
                }
            }
        }
    }

    private static class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable word : values) {
                sum += word.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }


}
