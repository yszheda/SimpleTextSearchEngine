import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.*;

public class InvertedIndexPartitioner implements Partitioner<InvertedIndexKey, Text>
{
	public int getPartition(InvertedIndexKey key, Text value, int numPart) 
	{
		return (key.getWord().hashCode()) % numPart;
	}   
	public void configure(JobConf arg0) {}   
}

