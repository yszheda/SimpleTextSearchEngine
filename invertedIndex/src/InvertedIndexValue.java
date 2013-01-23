import java.util.*;
import org.apache.hadoop.io.*;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class InvertedIndexValue implements Writable { 
		private HashMap<String, String> map = new HashMap<String, String>();

     	public InvertedIndexValue() {}

		public InvertedIndexValue(HashMap<String, String> map) {
				this.map = map;
		}

		/*
		public InvertedIndexValue(HashMap<String, HashSet<Integer>> map) {
				this.map = map;
		}

		public InvertedIndexValue(String file_name, Integer term_freq) {
				this.map.put(file_name, term_freq);
		}

		private Integer getTermFreq(String file_name) {
				return map.get(file_name);
		}
		*/

		public void insert(String file_name, String word_info)
		{
				this.map.put(file_name, word_info);
		}

		private String getString(String file_name) {
				return map.get(file_name);
		}

    	public void write(DataOutput out) throws IOException 
	 	{ 
				Iterator<String> it = map.keySet().iterator();
				while( it.hasNext() ) {
						String file_name = it.next();
						/*
						Integer term_freq = getTermFreq(file_name);
						new Text(file_name).write(out);
						new IntWritable(term_freq).write(out);
						*/
						String word_info = getString(file_name);
						new Text(file_name).write(out);
						new Text(word_info).write(out);
				}
     	} 

     	public void readFields(DataInput in) throws IOException 
	 	{
				Iterator<String> it = map.keySet().iterator();
				while( it.hasNext() ) {
						String file_name = it.next();
						/*
						Integer term_freq = getTermFreq(file_name);
						new Text(file_name).readFields(in);
						new IntWritable(term_freq).readFields(in);
						*/
						String word_info = getString(file_name);
						new Text(file_name).readFields(in);
						new Text(word_info).readFields(in);
				}
	 	}

     	public String toString()  
	 	{
				// document freq: map.size()
				String value_string = " " + map.size() + " ";
				SortedSet<String> keys = new TreeSet<String>(map.keySet());
				for(String file_name: keys)
				{
						String word_info = getString(file_name);
						value_string += ( file_name + "[" + word_info + "]" );
				}
				return value_string;
	 	}
}

