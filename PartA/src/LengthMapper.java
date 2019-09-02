
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;


public class LengthMapper extends Mapper<Object, Text, Text, IntWritable>  {

private IntWritable one	=  new IntWritable(1);

  public void map(Object key, Text line, Context context) throws IOException , InterruptedException
	{
		String[] fields = line.toString().split(";");
    if(fields.length==4 && fields[2].length()<141)
    {
		      String range = getRange(fields[2].length());

		      context.write(new Text(range),one);
        
    }
	}

	public String getRange(int length)
	{
    int min = 1;
		int max = 5;

        String range = "";

        while (min < 141)
		    {
            if (length <= max && length >= min)
			      {
                range = min + "-" + max;
                break;
            }
			      else
		        {
				          min += 5;
                  max += 5;
			      }
        }
		    return range;
	}
}
