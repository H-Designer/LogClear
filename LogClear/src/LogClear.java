import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class LogClear {
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String imei = new String();
    private String areacode  = new String();
    private String responsedata = new String();
    private String requesttime = new String();
    private String requestip = new String();
 
//    map阶段的key-value对的格式是由输入的格式所决定的，如果是默认的TextInputFormat，则每行作为一个记录进程处理，其中key为此行的开头相对于文件的起始位置，value就是此行的字符文本
//    map阶段的输出的key-value对的格式必须同reduce阶段的输入key-value对的格式相对应
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString());
    	String line=value.toString();  
        //System.out.println(line);  
        String arr[]=line.split(" ");
        imei=arr[0];
        areacode=arr[3];
        responsedata=arr[4];
        requesttime=arr[9];
        requestip=arr[11];
        boolean status = requestip.contains("video");
        if(status)
        	requestip="video";
        boolean status1 = requestip.contains("article");
        if(status1)
        	requestip="article";
     if(requesttime!="0"&&(status||status1))
     {
       String wd=new String();
       wd=imei+"\t"+areacode+responsedata+"\t"+"10"+"\t"+requesttime+"\t"+requestip;
       //wd="areacode|"+areacode +"|imei|"+ imei +"|responsedata|"+ responsedata +"|requesttime|"+ requesttime +"|requestip|"+ requestip;
       word.set(wd);
       context.write(word, one);
     }
    }
  }
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
  //  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    String[] otherArgs=new String[]{"hdfs://localhost:9000/JM/input","hdfs://localhost:9000/JM/output/LogClear"};
    
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
	//Job job = new Job(conf, "word count");
    Job job = Job.getInstance(conf);
    job.setJarByClass(LogClear.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
