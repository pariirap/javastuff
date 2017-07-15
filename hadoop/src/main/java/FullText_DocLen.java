import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class FullText_DocLen {

	public static class DocLen_Mapper
			extends Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */
	{

		Set<String> stopWords;

		public static final String HDFS_STOPWORD_LIST = "/users/PariRajaram/local/stop_words.txt";

		public static final String LOCAL_STOPWORD_LIST = "/Users/PariRajaram/stopwords.txt";

		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				String stopwordCacheName = new Path(HDFS_STOPWORD_LIST).getName();
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if (null != cacheFiles && cacheFiles.length > 0) {
					for (Path cachePath : cacheFiles) {
						if (cachePath.getName().equals(stopwordCacheName)) {
							loadStopWords(cachePath);
							break;
						}
					}
				}
			} catch (IOException ioe) {
				System.err.println("IOException reading from distributed cache");
				System.err.println(ioe.toString());
			}
		}

		void loadStopWords(Path cachePath) throws IOException {
			// note use of regular java.io methods here - this is a local file
			// now
			BufferedReader wordReader = new BufferedReader(new FileReader(cachePath.toString()));
			try {
				String line;
				System.out.println("stopword init");
				stopWords = new HashSet<String>();
				while ((line = wordReader.readLine()) != null) {
					stopWords.add(line);
				}
			} finally {
				wordReader.close();
			}
		}

		// Map function
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int fieldLen = 0;

			// static Map<String, Integer> wordMap = new HashMap();
			// reporter.incrCounter(RecordCounters.DOCLEN, 1);
			
			String line = value.toString();
			//
			StringTokenizer strToken = new StringTokenizer(line, " , ");
			//
			String token;

			//
			String docId = strToken.nextToken();
			int count=0;
			while (strToken.hasMoreTokens()) {

				token = strToken.nextToken();
				StringTokenizer strToken2 = new StringTokenizer(token, "\";/ -\'");
				while (strToken2.hasMoreTokens()) {
					String word = strToken2.nextToken();
					if (!stopWords.contains(word)) {
						
						count++;						
						
					}
				}
			}
			
			context.write(new Text(docId), new IntWritable(count));

		}
	}

}
