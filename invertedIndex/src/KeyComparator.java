import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*;
import java.util.*;


public class KeyComparator extends WritableComparator 
{
	protected KeyComparator() 
	{ 
		super(InvertedIndexKey.class, true); 
	}
	public int compare(WritableComparable w1, WritableComparable w2) 
	{
		InvertedIndexKey t1 = (InvertedIndexKey) w1;
		InvertedIndexKey t2 = (InvertedIndexKey) w2;
		return t1.compareTo(t2);
	}
}


