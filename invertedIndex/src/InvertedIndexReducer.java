import java.io.IOException;
import java.util.Iterator;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.HashMap;
import java.util.HashSet;

public class InvertedIndexReducer extends MapReduceBase
    implements Reducer<InvertedIndexKey, Text, Text, InvertedIndexValue> {

	private static final String seperator = ":";
	private String word = null;
	private int doc_freq = 0;
	private String line_offset = "";
	private String word_info = "";
	private HashMap<String, String> map = new HashMap<String, String>();

	@Override
	public void reduce(InvertedIndexKey key, Iterator<Text> values,
				   	OutputCollector<Text, InvertedIndexValue> output, Reporter reporter) throws IOException 
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
	        if( word != null && !key.getWord().equals(word) )
			{
					output.collect(new Text(word), new InvertedIndexValue(map)); 
					map = new HashMap<String, String>();
			}
			word = key.getWord();
			map.put( key.getFileName(), word_info );
	}
}

