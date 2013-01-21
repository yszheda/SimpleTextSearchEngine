import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndexCombiner extends MapReduceBase
    implements Reducer<InvertedIndexKey, Text, InvertedIndexKey, Text> {

	private static final String seperator = ":";
	private int doc_freq = 0;
	private String line_offset = "";
	private String word_info = "";

	@Override
	public void reduce(InvertedIndexKey key, Iterator<Text> values,
				   	OutputCollector<InvertedIndexKey, Text> output, Reporter reporter) throws IOException 
	{
			doc_freq = 0;
			line_offset = "";
			word_info = "";
			while(values.hasNext())
			{
					String[] token = values.next().toString().split(seperator);
					doc_freq += Integer.parseInt(token[0]);
					if( !line_offset.equals("") )
					{
							line_offset += ",";
					}
					line_offset += token[1];
			}
			word_info = doc_freq + seperator + line_offset;

			output.collect(key, new Text(word_info));
	}
}

