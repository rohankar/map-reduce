package cmu.ds.mr.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import cmu.ds.mr.conf.JobConf;
import cmu.ds.mr.io.OutputCollector;
import cmu.ds.mr.mapred.JobClient;
import cmu.ds.mr.mapred.MapReduceBase;
import cmu.ds.mr.mapred.Mapper;
import cmu.ds.mr.mapred.Reducer;

public class Anagram {

  public static class AnagramMapper extends MapReduceBase implements
          Mapper<Long, String, String, String> {

    public void map(Long key, String value, OutputCollector<String, String> output)
            throws IOException {
      String word = value.toLowerCase().toString();
      char[] wordChars = word.toCharArray();
      Arrays.sort(wordChars);
      String sortedWord = new String(wordChars);
      output.collect(sortedWord, word);
    }

  }

  public static class AnagramReducer extends MapReduceBase implements
          Reducer<String, String, String, String> {

    public void reduce(String key, Iterator<String> values, OutputCollector<String, String> output)
            throws IOException {
      String anastr = "";
      while (values.hasNext()) {
        String anagam = values.next();
        anastr = anastr + anagam + "~";
      }
      StringTokenizer outputTokenizer = new StringTokenizer(anastr, "~");
      if (outputTokenizer.countTokens() >= 2) {
        anastr = anastr.replace("~", "\t");
        output.collect(key, anastr);
      }
    }

  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: Anagram <inPath> <outPath> <numReducer>");
      return;
    }

    JobConf conf = new JobConf();
    conf.setJobName("anagramcount");

    conf.setMapperClass(AnagramMapper.class);
    conf.setReducerClass(AnagramReducer.class);

    conf.setInpath(args[0]);
    conf.setOutpath(args[1]);

    conf.setNumReduceTasks(Integer.parseInt(args[2]));

    JobClient.runJob(conf);

  }

}
