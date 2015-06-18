package enemMedia;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MediaEnemReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)throws IOException, InterruptedException {
		
		int 	cont = 0;
		float 	soma = 0f;

		for (FloatWritable value : values) {
		    cont++;
			soma = soma + value.get();
		}
		
		float media =   soma / cont ;
		
		context.write(key, new FloatWritable(media));
	}
}