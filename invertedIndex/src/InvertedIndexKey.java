import java.util.Formatter;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.*;

//public class InvertedIndexKey implements WritableComparable {
public class InvertedIndexKey implements WritableComparable<InvertedIndexKey> {
		private String word;
		private String file_name;

		public InvertedIndexKey() {}

		public InvertedIndexKey(String word, String file_name) {
				this.word = word;
				this.file_name = file_name;
		}

		public InvertedIndexKey(InvertedIndexKey other) {
				this.word = other.word;
				this.file_name = other.file_name;
		}

		public String getWord() {
				return word;
		}

		public String getFileName() {
				return file_name;
		}
		
		public void write(DataOutput out) throws IOException { 
				out.writeUTF(word);
				out.writeUTF(file_name);
		} 
		
		public void readFields(DataInput in) throws IOException {
				word = in.readUTF();
				file_name = in.readUTF();
		}

		public String toString() {
				return word + " " + file_name;
		}

		public int compareTo(InvertedIndexKey other) {
				String my_word = getWord();
				String other_word = other.getWord();
				String my_file_name = getFileName();
				String other_file_name = other.getFileName();

				if(my_word.compareTo(other_word) == 0) {
						return my_file_name.compareTo(other_file_name);
				} else {
						return my_word.compareTo(other_word);
				}
		}

		public boolean equals(Object o) {
				InvertedIndexKey other = (InvertedIndexKey)o;
				return this.word.equals(other.word) && this.file_name.equals(other.file_name);
		}

		/* may not be used */
		public int hashCode() {
				return word.hashCode() ^ file_name.hashCode();
		}

}
