package roughTest;
import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;


public class EdgeTest extends BasicComputation<IntWritable,IntWritable,IntWritable,IntWritable>
{
	@Override
	public void compute(
			Vertex<IntWritable, IntWritable, IntWritable> v,
			Iterable<IntWritable> msg) throws IOException {
		// TODO Auto-generated method stub
		boolean changed=false;
		for(IntWritable messages :msg)
		{
			
			if(v.getValue().get() < messages.get())
			{
				v.setValue(messages);
				changed=true;
			}
		}	
		if(getSuperstep()==0 | changed)
		{
			sendMessageToAllEdges(v,new IntWritable(10));
		}
		v.voteToHalt();
	}
}
