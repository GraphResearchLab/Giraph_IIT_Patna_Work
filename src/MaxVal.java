
import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;


public class MaxVal extends BasicComputation<LongWritable,DoubleWritable,FloatWritable,DoubleWritable>
 {
	@Override
	public void compute(
			Vertex<LongWritable, DoubleWritable, FloatWritable> v,
			Iterable<DoubleWritable> msg) throws IOException {
		// TODO Auto-generated method stub
		boolean changed=false;
		for(DoubleWritable messages :msg)
		{
			
			if(v.getValue().get() < messages.get())
			{
				v.setValue(messages);
				changed=true;
			}
		}	
		if(getSuperstep()==0 | changed)
		{
			sendMessageToAllEdges(v,v.getValue());
		}
		v.voteToHalt();
	}
}
