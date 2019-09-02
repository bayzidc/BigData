import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;


public class PopularTimeMapper extends Mapper<Object, Text, Text, IntWritable>  {

private IntWritable one	=  new IntWritable(1);

  public void map(Object key, Text line, Context context) throws IOException , InterruptedException
	{
		String[] fields = line.toString().split(";");

    if(fields.length==4 && fields[2].length()<=140)
    {
      try
      {
        LocalDateTime dt = LocalDateTime.ofEpochSecond(Long.parseLong(fields[0])/1000, 0, ZoneOffset.ofHours(-3));


            Pattern regex = Pattern.compile("#([a-zA-Z0-9_]+)");

            Matcher item = regex.matcher(fields[2]);

            if(dt.getHour()==22)
            {

              while(item.find())
              {
                  context.write(new Text(item.group()),one);
              }
          }
      }
      catch(Exception e)
      {}
    }
	}
}
