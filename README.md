import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

public class ProcessUnits {

    // Lớp Mapper
    public static class E_EMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        // Hàm Map
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String lastToken = null;
            StringTokenizer s = new StringTokenizer(line, "\t");
            String year = s.nextToken();  // Trích xuất năm
            
            // Duyệt qua các token để lấy token cuối (avgprice)
            while (s.hasMoreTokens()) {
                lastToken = s.nextToken();
            }

            // Chuyển đổi token cuối cùng thành số nguyên (avgprice)
            int avgPrice = Integer.parseInt(lastToken);
            output.collect(new Text(year), new IntWritable(avgPrice));  // Thu thập kết quả năm và avgprice
        }
    }

    // Lớp Reducer
    public static class E_EReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        // Hàm Reduce
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int maxAvg = 30;  // Ngưỡng để so sánh
            int value = 0;

            // Duyệt qua từng giá trị và kiểm tra nếu lớn hơn 30
            while (values.hasNext()) {
                value = values.next().get();
                if (value > maxAvg) {
                    output.collect(key, new IntWritable(value));  // Xuất ra năm và giá trị tiêu thụ nếu lớn hơn 30
                }
            }
        }
    }

    // Hàm chính
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ProcessUnits.class);
        conf.setJobName("max_electricityunits");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(E_EMapper.class);
        conf.setReducerClass(E_EReduce.class);

        conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
        conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

        // Định nghĩa đường dẫn file input và output
FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Chạy job
        JobClient.runJob(conf);
    }
}
