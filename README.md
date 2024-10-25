import java.util.*; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*; 

public class bt1 {
    public static class E_EMapper extends MapReduceBase implements 
       Mapper<LongWritable, Text, Text, IntWritable> {
        
        // Map function 
        public void map(LongWritable key, Text value, 
                        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
            String line = value.toString(); 
            StringTokenizer tokenizer = new StringTokenizer(line, "\t"); 
            String year = tokenizer.nextToken(); 
            int sum = 0;
            int count = 0;
            
            // Tính tổng và đếm số giá trị tiêu thụ điện
            while (tokenizer.hasMoreTokens()) {
                int consumption = Integer.parseInt(tokenizer.nextToken());
                sum += consumption;
                count++;
            }
            
            int avgConsumption = sum / count;  // Tính mức tiêu thụ trung bình
            output.collect(new Text(year), new IntWritable(avgConsumption)); 
        }
    }
    
    public static class E_EReduce extends MapReduceBase implements 
        Reducer<Text, IntWritable, Text, IntWritable> {
        
        // Reduce function 
        public void reduce(Text key, Iterator<IntWritable> values, 
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
            int threshold = 30;
            
            while (values.hasNext()) { 
                int avgConsumption = values.next().get();
                if (avgConsumption > threshold) { 
                    output.collect(key, new IntWritable(avgConsumption)); 
                }
            }
        }
    }

    // Main function 
    public static void main(String[] args) throws Exception { 
        JobConf conf = new JobConf(bt1.class); 
        conf.setJobName("max_electricity_units"); 
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class); 
        conf.setMapperClass(E_EMapper.class); 
        conf.setCombinerClass(E_EReduce.class); 
        conf.setReducerClass(E_EReduce.class); 
        conf.setInputFormat(TextInputFormat.class); 
        conf.setOutputFormat(TextOutputFormat.class); 
        
        FileInputFormat.setInputPaths(conf, new Path(args[0])); 
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
        
        JobClient.runJob(conf); 
    } 
}
