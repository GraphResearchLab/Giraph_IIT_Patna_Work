package iitp.giraph.algo.utils;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

public class MaxDegreeOfGraph extends BasicComputation<LongWritable,LongWritable,LongWritable,LongWritable>
{

	@Override
	public void compute(Vertex<LongWritable, LongWritable, LongWritable> vertex,
			Iterable<LongWritable> msg) throws IOException {
		boolean changed=false;
		if(getSuperstep()==0)
		{
			vertex.setValue(new LongWritable(vertex.getNumEdges()));
			sendMessageToAllEdges(vertex,vertex.getValue());
		}
		else
		{
			for(LongWritable messages :msg)
			{
				
				if(vertex.getValue().get() < messages.get())
				{
					vertex.setValue(messages);
					changed=true;
				}
			}	
			if(getSuperstep()==0 | changed)
			{
				sendMessageToAllEdges(vertex,vertex.getValue());
			}
			vertex.voteToHalt();
		}
		
		
	}

}
