import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AttributesCountMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

        String unparsed = value.toString().split("\\|")[8];

        if(!unparsed.equals("null")) {
            
            unparsed = unparsed.substring(1, unparsed.length() - 1);
            
            String[] attributes = unparsed.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            
            for(String att : attributes) {
                
                String[] attSplit = att.split(":(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                
                String k = attSplit[0].replace("\"","").toLowerCase();
                String v = attSplit[1].replace("\"","").toLowerCase();

                if(v.equals("true")) {
                    context.write(new Text(k), new IntWritable(1));
                }
                else if(v.substring(0,1).equals("{")) {

                    v = v.replace("{", "").replace("}", "");

                    if(!v.equals("")) {
                        
                        String[] subAttributes = v.split(",");

                        for(String subAtt : subAttributes) {

                            String[] subAttSplit = subAtt.split(":");

                            String subK = subAttSplit[0].trim().replace("'","").toLowerCase();
                            String subV = subAttSplit[1].trim().replace("'","").toLowerCase();

                            if(subV.equals("true")) {
                                context.write(new Text(k + "-" + subK), new IntWritable(1));
                            }
                        }
                    }

                }

            }

        }
    }
}

