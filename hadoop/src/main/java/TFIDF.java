
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class TFIDF extends Configured implements Tool {

	public static class TFIDF_Partitioner extends Partitioner<Text, Text> {

		/**
		 * key => word@655121
		 */
		public int getPartition(Text key, Text value, int numPartitions) {

			String wordDocID = key.toString();

			StringTokenizer strTokenizer = new StringTokenizer(wordDocID, "@");

			if (strTokenizer.hasMoreElements()) {
				String word = strTokenizer.nextToken();
				return word.hashCode() % numPartitions;
			}

			return 0;
		}

	}

	public static final String HDFS_STOPWORD_LIST = "/users/PariRajaram/local/stop_words.txt";

	public static final String LOCAL_STOPWORD_LIST = "/Users/PariRajaram/stopwords.txt";

	void cacheStopWordList(JobConf conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(HDFS_STOPWORD_LIST);

		// upload the file to hdfs. Overwrite any existing copy.
		fs.copyFromLocalFile(false, true, new Path(LOCAL_STOPWORD_LIST), hdfsPath);

		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	}

	public static class FullText_Mapper
			extends Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			IntWritable> /* Output value Type */
	{

		Set<String> stopWords;

		static enum RecordCounters {
			DOCLEN
		};

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
			context.getCounter(RecordCounters.DOCLEN).increment(1);
			String line = value.toString();
			//
			StringTokenizer strToken = new StringTokenizer(line, " , ");
			//
			String token;

			//
			int docId = Integer.parseInt(strToken.nextToken());

			while (strToken.hasMoreTokens()) {

				token = strToken.nextToken();
				StringTokenizer strToken2 = new StringTokenizer(token, "\";/ -\'");
				while (strToken2.hasMoreTokens()) {
					String word = strToken2.nextToken();
					if (!stopWords.contains(word)) {
						word = word + "@" + docId;

						context.write(new Text(word), new IntWritable(1));
					}
				}
			}

		}
	}

	// Reducer class
	public static class FullText_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		static Map<String, Integer> map = new HashMap();

		// Reduce function
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int maxavg = 30;
			int val = 0;

			StringTokenizer strToken = new StringTokenizer(key.toString(), "@");

			String word = strToken.nextToken();
			String docId = strToken.nextToken();

			for (IntWritable v : values) {
				val += v.get();
			}

			context.write(key, new IntWritable(val));
		}
	}

	public static class FullText_Mapper2
			extends Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			Text> /* Output value Type */
	{

		// Map function
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int fieldLen = 0;

			String line = value.toString();
			StringTokenizer strToken = new StringTokenizer(line, "@  \t");
			String word = strToken.nextToken();

			String docId = strToken.nextToken();
			String count = strToken.nextToken();
			String val = docId + "/" + count.toString();

			context.write(new Text(word), new Text(val));

		}
	}

	// Reducer class
	public static class FullText_Reducer2 extends Reducer<Text, Text, Text, Text> {

		static Map<String, Integer> map = new HashMap();

		// Reduce function
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String val = " ";

			for (Text v : values) {
				val += v + " ";
			}
			val += " ";
			context.write(key, new Text(val));
		}
	}

	public static class FullText_Mapper3
			extends Mapper<LongWritable, /* Input key Type */
			Text, /* Input value Type */
			Text, /* Output key Type */
			Text> /* Output value Type */
	{
		static enum RecordCounters {
			DOCLEN
		};

		private long docLen;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			this.docLen = context.getConfiguration().getLong(FullText_Mapper3.RecordCounters.DOCLEN.name(), 0);
		}

		// Map function
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int fieldLen = 0;

			String line = value.toString();
			StringTokenizer strToken = new StringTokenizer(line, "@  \t");
			String word = strToken.nextToken();
			int docCount = 0;

			String val = "";

			while (strToken.hasMoreElements()) {
				docCount++;
				val += strToken.nextToken() + " ";
			}
			double idf = Math.log(this.docLen * 1.0 / docCount);

			val += " [ " + Double.toString(idf) + " ] ";

			context.write(new Text(word), new Text(val));

		}
	}



	public static void main(String[] args) {

		try {
			int returnStatus = ToolRunner.run(new TFIDF(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public int run(String[] args) throws Exception {
		// Configuration conf = getConf();
		Job job = new Job();
		job.setJarByClass(TFIDF.class);
		job.setJobName("TFIDF");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(FullText_Mapper.class);
		job.setCombinerClass(FullText_Reducer.class);
		job.setReducerClass(FullText_Reducer.class);
		job.setPartitionerClass(TFIDF_Partitioner.class);
		cacheStopWordList((JobConf) job.getConfiguration());
		boolean status = job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter docLenCounter = counters.findCounter(TFIDF.FullText_Mapper.RecordCounters.DOCLEN);
		System.out.println("DOC LEN = " + docLenCounter.getValue());
		System.out.println("Job1 status " + status);

		Job job2 = new Job();
		job2.setJarByClass(TFIDF.class);
		job2.setJobName("TFIDF2");

		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.getConfiguration().setLong(FullText_Mapper3.RecordCounters.DOCLEN.name(), docLenCounter.getValue());
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(FullText_Mapper2.class);
		job2.setCombinerClass(FullText_Reducer2.class);
		job2.setReducerClass(FullText_Reducer2.class);

		boolean status2 = job2.waitForCompletion(true);
		System.out.println("Job2 status " + status2);

		Job job3 = new Job();
		job3.setJarByClass(TFIDF.class);
		job3.setJobName("TFIDF3");

		FileInputFormat.setInputPaths(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));

		job3.getConfiguration().setLong(FullText_Mapper3.RecordCounters.DOCLEN.name(), docLenCounter.getValue());
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setMapperClass(FullText_Mapper3.class);

		boolean status3 = job3.waitForCompletion(true);
		System.out.println("Job3 status " + status3);

		Job job4 = new Job();
		job4.setJarByClass(FullText_DocLen.class);
		job4.setJobName("TFIDF DOCLEN");

		FileInputFormat.setInputPaths(job4, new Path(args[0]));
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));

		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(IntWritable.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(IntWritable.class);

		job4.setMapperClass(FullText_DocLen.DocLen_Mapper.class);
		cacheStopWordList((JobConf) job4.getConfiguration());
		boolean status4 = job4.waitForCompletion(true);
		System.out.println("Job4 status " + status4);

		// theLogger.info("run(): status="+status);
		return status2 ? 0 : 1;
	}

}
