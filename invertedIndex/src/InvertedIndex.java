import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class InvertedIndex
{
   public static void main(String[] args) throws Exception 
   {
        JobConf conf = new JobConf(InvertedIndex.class);
        conf.setJobName("InvertedIndex");              
        
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapOutputKeyClass(InvertedIndexKey.class);
//        conf.setMapOutputValueClass(IntWritable.class);
//        conf.setMapOutputValueClass(WordInfo.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(InvertedIndexValue.class);
        
        conf.setNumReduceTasks(1);

        conf.setMapperClass(InvertedIndexMapper.class);
        conf.setCombinerClass(InvertedIndexCombiner.class);          
        conf.setReducerClass(InvertedIndexReducer.class);          
        conf.setPartitionerClass(InvertedIndexPartitioner.class);
        conf.setOutputKeyComparatorClass(KeyComparator.class);
        conf.setOutputValueGroupingComparator(GroupComparator.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
   }
}

