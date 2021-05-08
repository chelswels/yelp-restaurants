import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortSuccessfulMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

        String[] columns = value.toString().split("\\|");

        double rating = Double.parseDouble(columns[5]);
        int reviews = Integer.parseInt(columns[6]);

        if(rating >= 4 && reviews >= 20 && columns[7].equals("1")) {
            context.write(new Text(value.toString()), new Text(""));
        }
    }
}