
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

public class SportsMapper extends Mapper<Object, Text, Text, IntWritable> {

	private ArrayList<String> athlete;
	private ArrayList<String> sport;

	private IntWritable one = new IntWritable(1);

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException
	{
		String[] fields = line.toString().split(";");

		if(fields.length==4 && fields[2].length()<=140)
		{
			String message = fields[2].toLowerCase();

			int size = athlete.size();

			for(int i=0; i<size; i++)
			{
				try
				{
					if(message.contains(athlete.get(i).toLowerCase()))
					{
						   context.write(new Text(sport.get(i)), one);
					}
				}
			catch(Exception e)
			{}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athlete = new ArrayList<String>();
		sport = new ArrayList<String>();
		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try
		{
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null)
			{
				String[] fields = line.split(",");
				if(fields.length==11 && !fields[1].isEmpty() && !fields[7].isEmpty())
				{
					athlete.add(fields[1]);
					sport.add(fields[7]);
				}
			}
			br.close();

		}
		catch (IOException e1)
		{	}
		super.setup(context);
	}
}
