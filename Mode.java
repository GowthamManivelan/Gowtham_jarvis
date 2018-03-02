import java.io.IOException;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import java.util.ArrayList;

public class Mode  extends Configured implements Tool {

  public static class ModeMapper
      extends Mapper<LongWritable, Text, LongWritable, Records> {

    private Records Temp = null;
    public void map(LongWritable k, Text v, Context context) {
      Records record = new Records();
      try {
        record.parse(v);
} catch (ParseError pe) {
        // Got a malformed record. Ignore it.
        return;
      } 
      
      try{
      
      Integer temperature = Integer.parseInt(record.get_temp());
      if (null == temperature) {
        return;
      } else {
        if (temperature.intValue() != 9999) {
          Temp = record;
        } 
      } 
    }catch(Exception e){
      e.getMessage();
    } 
}   
 public void cleanup(Context context)
        throws IOException, InterruptedException {
      if (null != avgTemp) {
        context.write(new LongWritable(0), Temp);
      }
    }
  }

  public static class ModeReducer
      extends Reducer<LongWritable, Records, FloatWritable,NullWritable> {

    public void reduce(LongWritable k, Iterable<Records> vals, Context context)
        throws IOException, InterruptedException {

      int maxValue= 0;
      int maxCount= 0;
      ArrayList<Integer> mode = new ArrayList<Integer>();
      for (Records r : vals) {
        mode.add(Integer.parseInt(r.get_temp()));
      }
        for(int i=0;i<mode.size();++i){
        int count=0;
 for (int j=0;j<mode.size();++j){
        if(mode.get(j)==mode.get(i))
        ++count;
}
if(count > maxCount){
maxCount = count;
maxValue= mode.get(i);
}
}


      context.write(new FloatWritable(maxValue), NullWritable.get());
    }
  }
public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(Mode.class);

    job.setMapperClass(ModeMapper.class);
    job.setReducerClass(ModeReducer.class);

    FileInputFormat.addInputPath(job, new Path("record"));
    FileOutputFormat.setOutputPath(job, new Path("modeoftemp1"));

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Records.class);

    job.setOutputKeyClass(Records.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);
if (!job.waitForCompletion(true)) {
      return 1; // error.
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new Mode(), args);
    System.exit(ret);
  }
}