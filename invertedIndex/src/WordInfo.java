import java.util.*;
import org.apache.hadoop.io.*;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class WordInfo implements Writable { 
		private HashSet<String> line_offset_set = new HashSet<String>();

     	public WordInfo () {}

		public WordInfo (WordInfo other) {
				this.line_offset_set = other.line_offset_set;
		}

		public WordInfo (HashSet<String> line_offset_set) {
				this.line_offset_set = line_offset_set;
		}

		public WordInfo (String line_offset) {
				this.line_offset_set.add(line_offset);
		}

		public HashSet<String> getLineOffsetSet() {
				return line_offset_set;
		}

		public void insertLineOffset(String line_offset) {
				this.line_offset_set.add(line_offset);
		}

		public void merge(HashSet<String> new_line_offset_set) {
				this.line_offset_set.addAll(new_line_offset_set);
		}

		public void merge(WordInfo new_word_info) {
				this.line_offset_set.addAll(new_word_info.line_offset_set);
				//test
				this.line_offset_set.add("test");
		}

		private int getLineOffsetNum() {
				return line_offset_set.size();
		}

    	public void write(DataOutput out) throws IOException 
	 	{ 
				Iterator<String> it = line_offset_set.iterator();
				while( it.hasNext() ) {
						String line_offset = it.next();
//						new Text(line_offset).write(out);
						out.writeUTF(line_offset);
				}
     	} 

     	public void readFields(DataInput in) throws IOException 
	 	{
				Iterator<String> it = line_offset_set.iterator();
				while( it.hasNext() ) {
						String line_offset = it.next();
						new Text(line_offset).readFields(in);
//						line_offset = in.readUTF();
				}
	 	}

     	public String toString()  
	 	{
				// term freq: line_offset_set.size()
				String word_info_string = " " + getLineOffsetNum() + " [";
				SortedSet<String> line_offsets = new TreeSet<String>(line_offset_set);
				for(String line_offset : line_offsets)
				{
						word_info_string += ( line_offset + "," );
				}
				word_info_string += "] ";
				return word_info_string;
	 	}
}

