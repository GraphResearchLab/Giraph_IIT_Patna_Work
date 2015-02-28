package iitp.giraph.io;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/*Output Format
 * VertexId <tab> VertexValue
 * Text <tab> Text
 * */


public class LongLongVertexIdValueFileOutputFormat extends TextVertexOutputFormat<LongWritable,LongWritable,LongWritable>
{

	@Override
	public TextVertexOutputFormat<LongWritable, LongWritable, LongWritable>.TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
		
		return new LongLongLongLongOutputFormat();
	}

	class LongLongLongLongOutputFormat extends TextVertexWriterToEachLine
	{
		private String delimiter="\t";
		@Override
		protected Text convertVertexToLine(Vertex<LongWritable, LongWritable, LongWritable> vertex)throws IOException {
			
			StringBuilder builder=new StringBuilder();
			builder.append(vertex.getId().toString());
			builder.append(delimiter);
			builder.append(vertex.getValue());
			
			return new Text(builder.toString());
		}
		
	}
}
