package com.lab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.parse.ParseText;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InvertedLink extends Configured implements Tool{
	private static Logger logger = LoggerFactory.getLogger(InvertedLink.class);
	
	public static class TokenizerMapper extends Mapper<Text, ParseText, Text, Text> {
	    private static final Text one = new Text("1");
        private Text word = new Text();
        
        Set<String> stopWords = new HashSet<>();
        
        protected void setup(Context context) throws IOException, InterruptedException{
        	Configuration conf = context.getConfiguration();
        	
        	if(conf.getBoolean("wordcount.skip.patterns", false)){
                URI[] localPaths = context.getCacheFiles();
                Path path = new Path(localPaths[0]);

                logger.info("Precessing cached file: " + path);

                try{
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    FSDataInputStream in = fs.open(path);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String pattern;
                    logger.info("adding stopWords to hashset");
                    while((pattern = br.readLine()) != null){
                        stopWords.add(pattern);
                    }
                }catch (IOException ex){
                    System.err.println("Caught exception while parsing the cached file: " + path);
                }
            }
        	
        }
        
        public void map(Text key, ParseText value, Context context) throws IOException, InterruptedException {
        	//  String line = value.toString().replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness|.|,|\\?|'|:|;) ", " ").toLowerCase();
        	  String line = value.toString().toLowerCase();
              StringTokenizer itr = new StringTokenizer(line);
              String url = key.toString();
              
              while (itr.hasMoreTokens()) {
            	    String token = itr.nextToken();
            	    if(stopWords.contains(token))
            	    	continue;
                    StringBuilder st = new StringBuilder();
                    st.append(token);
                    st.append(" ");
                    st.append(url);
                    
                    word.set(st.toString());
                    context.write(word, one);
              }
        }
  }
    public static class TokenizerCombiner extends Reducer<Text, Text, Text, Text>{
	  private Text outKey = new Text();
	  private Text outValue = new Text();
	  
	  @Override
	  protected void reduce(Text key,  Iterable<Text> values, Context context)throws IOException, InterruptedException {
		  StringTokenizer tokenizer = new StringTokenizer(key.toString());
		if(tokenizer.countTokens()<2) {  
		  StringBuilder sb = new StringBuilder();
		  Iterator<Text> iter = values.iterator();
		  while(iter.hasNext()) {
			  sb.append(iter.next().toString());
			  if(iter.hasNext()) {
				  sb.append(" ");
			  }
		  }
		  outValue.set(sb.toString());
		  context.write(key, outValue);
	  }else {
		 //process from map
		  String word =tokenizer.nextToken();
		  String url = tokenizer.nextToken();
		  
		  int sum=0;
		  for(Text value: values) {
			  sum+=Integer.parseInt(value.toString());
		  }
		  outKey.set(word);
		  StringBuilder sb = new StringBuilder();
		  sb.append(url);
		  sb.append("=");
		  sb.append(sum);
		  outValue.set(sb.toString());
		  context.write(outKey, outValue);  
	  }
	 }
  }
  
  
  public static class TokenizerReducer extends Reducer<Text, Text, Text, Text> {
        private Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        	StringBuilder sb = new StringBuilder();
        	Iterator<Text> iter= values.iterator();
        	while(iter.hasNext()) {
        		sb.append(iter.next().toString());
        		if(iter.hasNext()) {
        			sb.append(" ");
        		}
        	}
        	outValue.set(sb.toString());
        	context.write(key, outValue);
        }
  }
   
  
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new InvertedLink(), args);
  }
    
   public Path[] filePaths(String in, FileSystem fs)throws IOException{
       List<Path> pathList = new ArrayList<>();
       this.recurse(in, fs, pathList);
       FileStatus[] fss = fs.listStatus(pathList.toArray(new Path[pathList.size()]), new InvertedPathFilter());
       Path[] paths = FileUtil.stat2Paths(fss);
       return paths;
   }
   
   private void recurse(String in, FileSystem fs, List<Path> pathList) throws IOException{
       FileStatus[] globStatus = fs.listStatus(new Path(in));

       Path[] stat2Paths = FileUtil.stat2Paths(globStatus);

       for (int i = 0; i < stat2Paths.length; i++) {

           if (fs.isDirectory(stat2Paths[i])) {
               recurse(stat2Paths[i].toString(), fs, pathList);
           }else{
               pathList.add(stat2Paths[i]);
           }
       }
   }

@Override
public int run(String[] args) throws Exception {
	Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
         System.err.println("Usage: wordcount <in> [<in>...] <out>");
          System.exit(2);
    }
    
    Job job = Job.getInstance(getConf());
    
    for(int i=0;i<args.length;i++) {
    	if("-skip".equals(args[i])) {
    		job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
    		i+=1;
    		job.addCacheFile(new URI("hdfs://node1:9000"+ args[i]));
    		logger.info("added stop words file to the distributed cache: " + args[i]);
    	}
    }
    
    job.setJarByClass(InvertedLink.class);    
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(TokenizerCombiner.class);
    job.setReducerClass(TokenizerReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setInputFormatClass(CombineSequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    logger.info("input file: " +args[0]);
    logger.info("output file: " + args[1]);
    
    FileSystem fs = FileSystem.newInstance(getConf());
    FileInputFormat.setInputPaths(job, this.filePaths(args[0],fs));
    FileInputFormat.setInputDirRecursive(job,true);
    FileInputFormat.setInputPathFilter(job, InvertedPathFilter.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	return 0;
}
}
