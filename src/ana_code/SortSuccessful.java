import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortSuccessful {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: SortSuccessful <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(SortSuccessful.class);
        job.setJobName("Parse into comma delimited format, filter out null values, and drop unneeded columns in input file");
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SortSuccessfulMapper.class);
        job.setReducerClass(SortSuccessfulReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}