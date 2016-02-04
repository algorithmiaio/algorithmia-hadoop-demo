package algorithmia.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import com.algorithmia.APIException;
import com.algorithmia.Algorithmia;
import com.algorithmia.AlgorithmiaClient;
import com.algorithmia.algo.AlgoFailure;
import com.algorithmia.algo.AlgoResponse;
import com.algorithmia.algo.AlgoSuccess;
import com.algorithmia.algo.Algorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Modified WordCount from Hadoop documentation where each line from the input file is called to HelloWorld algorithm
 * and the results of the algorithm call are counted
 */
public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private static Algorithm algo;

    protected void setup(Context context) {
      /* Perform client initialization here, handle exceptions e.g. auth errors */
      AlgorithmiaClient client = Algorithmia.client("/* YOUR API KEY */");
      algo = client.algo("algo://pmcq/Hello/0.1.3");
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      try {
        AlgoResponse response = algo.pipeJson(value.toString());
        if (response.isFailure()) {
          /* Don't output the failure, but maybe log it, increment Hadoop counters etc */
          System.err.println("Algo Failure! " + ((AlgoFailure)response).error.getMessage());
        } else {
          /* Output the successful algorithm call */
          word.set(((AlgoSuccess)response).asString());
          context.write(word, one);
        }
      } catch (APIException e) {
        /* Retry the API call if there's an issue */
        System.err.println("API Failure! " + e.getMessage());
      }

    }
  }

  /**
   * Untouched from Hadoop Sample code
   */
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    /* Set API endpoint if not connecting to prod */
    // conf.set("mapred.child.env", "ALGORITHMIA_API=http://localhost:9000");

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:60812/home/patrick/testdata"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:60812/home/patrick/outputdata"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}