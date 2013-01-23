import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;

public class InvertedIndexMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, InvertedIndexKey, Text> {

	private static final String seperator = ":";
	private InvertedIndexKey inverded_index_key;
	private Integer doc_freq = 1;
	private String line_offset;
	private String word_info;
  
	@Override
	public void map(LongWritable key, Text value, 
	    OutputCollector<InvertedIndexKey, Text> output, Reporter reporter) throws IOException {
	        
	  String line = value.toString(); 
	  line_offset = key.toString();
	  word_info = doc_freq + seperator + line_offset;
	
	  FileSplit split = (FileSplit)(reporter.getInputSplit());		
	  String file_name = split.getPath().getName();
	
	  // need to implement a tokenizer
	  // maybe regex
	  StringTokenizer itr = new StringTokenizer(line, " \t\n\r\f<>{}()[]&*#,.:;<>+-?///!'/0123456789");
	  while(itr.hasMoreTokens()) {
	  	String token = itr.nextToken().trim();
		inverded_index_key = new InvertedIndexKey(token, file_name);
	  	output.collect(inverded_index_key, new Text(word_info));

	  }
	}
	
	/*
	public void configure(JobConf conf)
	{
	  	  String input_path = new String(conf.get("map.input.file"));
	}
	*/

} 

