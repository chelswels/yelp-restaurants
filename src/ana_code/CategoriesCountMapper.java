import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoriesCountMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

        String unparsed = value.toString().split("\\|")[9];

        String[] categories = unparsed.replace("\"", "").split(",");

        for (String cat : categories) {
            context.write(new Text(cat.trim()), new IntWritable(1));
        }
    }
}