import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleaningMapper
extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

        String parsed = value.toString().replace("{\"business_id\":", "").replace(",\"name\":", "|").replace(",\"address\":", "|")
                          .replace(",\"city\":", "|").replace(",\"state\":", "|").replace(",\"postal_code\":", "|")
                          .replace(",\"latitude\":", "|").replace(",\"longitude\":", "|").replace(",\"stars\":", "|")
                          .replace(",\"review_count\":", "|").replace(",\"is_open\":", "|").replace(",\"attributes\":", "|")
                          .replace(",\"categories\":", "|").replace(",\"hours\":", "|").replace("}}", "}");

        String[] columns = parsed.toString().split("\\|");

        if (columns.length != 14) {
            return;
        }

        for (int i = 0; i < 11; i++) {
            columns[i] = columns[i].replace("\"", "");
        }

        String output = "";

        if (!columns[0].equals("") && !columns[1].equals("") && !columns[3].equals("") && !columns[5].equals("") 
           && !columns[8].equals("") && !columns[9].equals("") && !columns[10].equals("")) {
        
            output = String.join("|", columns[0], columns[1], columns[3], columns[4], columns[5], columns[8], columns[9], columns[10], columns[11], columns[12]);

            context.write(new Text(output), new Text(""));
        
        }
    }
}