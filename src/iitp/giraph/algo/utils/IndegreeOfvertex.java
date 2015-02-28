package iitp.giraph.algo.utils;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;

public class IndegreeOfvertex extends BasicComputation<LongWritable,LongWritable,LongWritable,LongWritable>
{

	@Override
	public void compute(Vertex<LongWritable, LongWritable, LongWritable> vertex,
			Iterable<LongWritable> msg) throws IOException {
		// TODO Auto-generated method stub
		vertex.setValue(new LongWritable(vertex.getNumEdges()));
		vertex.voteToHalt();
	}
	
}
