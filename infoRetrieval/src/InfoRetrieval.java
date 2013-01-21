import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.Map;
import java.util.Comparator;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.*;

class ValueComparator implements Comparator<String>
{
		Map<String, Double> base;
		public ValueComparator(Map<String, Double> base)
		{
				this.base = base;
		}

		public int compare(String a, String b)
		{
				if(base.get(a) >= base.get(b))
				{
						return -1;
				}
				else
				{
						return 1;
				}
		}
}

public class InfoRetrieval
{
		private HashMap<String, Double> doc_weight_map = new HashMap<String, Double>();
		private String src_path = "";
		private String index_file_name = "";
		private String search_word = "";
		private String search_command;
		private String[] search_words;
//		private String output_path = "";
		private Configuration conf;
		private FileSystem fs;
		private Path src_doc_path;
		private Path index_file_path;
		private boolean ignore_case_flag = false;

		public enum SetOperation{  
				Union, Intersection
		}  

		private SetOperation op = SetOperation.Union;

		public InfoRetrieval()  throws Exception
		{
				conf = new Configuration();
				fs = FileSystem.get(conf);
		}

		private void searchDoc(String file_name) throws Exception
		{
				Path file_name_path = new Path(src_path + "/" + file_name);
				FSDataInputStream in = fs.open(file_name_path);
				BufferedReader br_in = new BufferedReader(new InputStreamReader(in));
				String message_in;
				int line_num = 0;
			    while( (message_in = br_in.readLine()) != null)
				{
						line_num++;
						for(String search_word : search_words)
						{
								String compared_message_in = new String(message_in);
								if(ignore_case_flag == true)
								{
										compared_message_in = compared_message_in.toLowerCase();
										search_word = search_word.toLowerCase();
								}

								String pattern_string = new String("\\b(" + search_word +")\\b");
								Pattern pattern = Pattern.compile(pattern_string);
								Matcher matcher = pattern.matcher(compared_message_in);
//								if(message_in.indexOf(search_word) != -1)
								if(matcher.find())
								{
										System.out.println("line" + line_num + ":" + message_in);
//										System.out.println("line" + line_num + ":" + matcher.group(1));
								}
						}
				}
		}

		private void show(int max_doc_num) throws Exception
		{
				if(doc_weight_map.size() != 0)
				{
						System.out.println(doc_weight_map.size() + " acticles contain the word(s)!");
						ValueComparator value_compr = new ValueComparator(doc_weight_map);
						TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(value_compr);
						sorted_map.putAll(doc_weight_map);
						int doc_num = 0;
						for(String file_name: sorted_map.keySet())
						{
								if(doc_num < max_doc_num)
								{
										Double tf_idf = doc_weight_map.get(file_name);
										System.out.println(file_name + ": score=" + tf_idf);
										searchDoc(file_name);
								}
								doc_num++;
						}
				}
				else
				{
						System.out.println("the word(s) cannot be found!");
				}
		}

		public HashMap<String, Double> searchSingleWord(String search_word) throws Exception
		{
				HashMap<String, Double> doc_weight_map = new HashMap<String, Double>();

				Path src_doc_path = new Path(src_path);
				FileStatus[] src_doc_list = fs.listStatus(src_doc_path);
				final int doc_num = src_doc_list.length;

				Path index_file_path = new Path(index_file_name);
				FSDataInputStream in = fs.open(index_file_path);
				BufferedReader br_in = new BufferedReader(new InputStreamReader(in));
				String message_in;
			    while( (message_in = br_in.readLine()) != null)
				{
					String[] word_and_info = message_in.split("\t");
					String word = word_and_info[0];

					if(ignore_case_flag == true)
					{
							word = word.toLowerCase();
							search_word = search_word.toLowerCase();
					}

					if(word.equals(search_word))
					{
						String[] word_info = word_and_info[1].split(" ");
						int doc_freq = Integer.parseInt(word_info[1].trim());
						String[] doc_and_info = word_info[2].split("\\]");

						for( int i=0; i<doc_freq; i++ )
						{
								String[] doc_and_info_token = doc_and_info[i].split("\\[");
								String file_name = doc_and_info_token[0];
								String[] info_token = doc_and_info_token[1].split(":");
								int term_freq = Integer.parseInt(info_token[0]);
								double tf_idf = term_freq * Math.log10(1.0 * doc_num / doc_freq);

								/*
								System.out.println(term_freq);
								System.out.println(doc_num);
								System.out.println(doc_freq);
								*/

								doc_weight_map.put(file_name, new Double(tf_idf));
						}
					}
				}
				in.close();
				return doc_weight_map;
		}

		public void search() throws Exception
		{
				doc_weight_map = new HashMap<String, Double>();

				Set<String> doc_weight_set = new HashSet<String>( doc_weight_map.keySet() );
				if(search_command.contains("-i"))
				{
						ignore_case_flag = true;
						search_command = search_command.replace("-i","").trim();
				}
				search_words = search_command.split(" ");
				for(String search_word : search_words)
				{
						if( search_word.equals("-o") )
						{
								op = SetOperation.Union;
						}
						else if( search_word.equals("-a") )
						{
								op = SetOperation.Intersection;
						}
//						if( !search_word.equals("-o") && !search_word.equals("-a") )
						else
						{
								HashMap<String, Double> result_map = searchSingleWord(search_word);
								Set<String> result_set = result_map.keySet();
								if( op == SetOperation.Union )
								{
										doc_weight_set.addAll(result_set);
								}
								if( op == SetOperation.Intersection )
								{
										doc_weight_set.retainAll(result_set);
								}
								for(String file_name: doc_weight_set)
								{
										Double tf_idf = doc_weight_map.get(file_name);
										Double result_tf_idf = result_map.get(file_name);
										if(tf_idf == null)
										{
												tf_idf = result_map.get(file_name);
												doc_weight_map.put(file_name, tf_idf);
										}
										else if(result_tf_idf != null)
										{
												Double new_tf_idf = tf_idf + result_tf_idf;
												doc_weight_map.remove(file_name);
												doc_weight_map.put(file_name, new_tf_idf);
										}
								}
								if( op == SetOperation.Intersection )
								{
										Set<String> file_name_set = doc_weight_map.keySet(); 
										Iterator<String> it = file_name_set.iterator();
										/*
										for(String file_name: doc_weight_map.keySet() )
										{
												if( !doc_weight_set.contains(file_name) )
												{
														doc_weight_map.remove(file_name);
												}
										}
										*/
										while(it.hasNext())
										{
												String file_name = it.next();
												if( !doc_weight_set.contains(file_name) )
												{
														it.remove();
												}
										}
								}
						}
				}
		}

		public static void main(String [] args) throws Exception
		{
				InfoRetrieval info_retrieval = new InfoRetrieval();
				info_retrieval.src_path = args[0];
				info_retrieval.index_file_name = args[1];
//				info_retrieval.search_word = args[2];
				while(true)
				{
					System.out.println("Enter the word you want to search:");
					BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//					info_retrieval.search_word = br.readLine();
					info_retrieval.search_command = br.readLine();
					info_retrieval.search();
					/*
					for(String search_word : search_words)
					{
						info_retrieval.search(search_word);
					}
					*/
					info_retrieval.show(10);
				}
		}
}
