import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortUnsuccessfulMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

        String[] columns = value.toString().split("\\|");

        double rating = Double.parseDouble(columns[5]);

        if(rating < 4 || columns[7].equals("0")) {
            context.write(new Text(value.toString()), new Text(""));
        }
    }
}