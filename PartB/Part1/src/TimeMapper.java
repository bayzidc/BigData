import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;


public class TimeMapper extends Mapper<Object, Text, IntWritable, IntWritable>  {

private IntWritable one	=  new IntWritable(1);

  public void map(Object key, Text line, Context context) throws IOException , InterruptedException
	{
		String[] fields = line.toString().split(";");
	 if(fields.length==4 && fields[2].length()<=140)
   {
     try
     {
		     int hour = getEachHour(Long.parseLong(fields[0]));
	       context.write(new IntWritable(hour),one);
     }
     catch(Exception e)
     {}
   }
	}

	public int getEachHour(Long time)
	{
		LocalDateTime dt = LocalDateTime.ofEpochSecond(time/1000, 0, ZoneOffset.ofHours(-3));
		return dt.getHour();
	}
}
