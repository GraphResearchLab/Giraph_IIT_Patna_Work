package iitp.giraph.algo.utils;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;

public class ConnectedComponents
		extends
		BasicComputation<LongWritable, LongWritable, LongWritable, LongWritable> {

	@Override
	public void compute(
			Vertex<LongWritable, LongWritable, LongWritable> vertex,
			Iterable<LongWritable> messages) throws IOException {
		// TODO Auto-generated method stub

		
		// First superstep is special, because we can simply look at the
		// neighbors
		if (getSuperstep() == 0) {
			vertex.setValue(new LongWritable(vertex.getId().get()));
			/*
			 * for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
			 * long neighbor = edge.getTargetVertexId().get(); if (neighbor <
			 * currentComponent) { currentComponent = neighbor; } } // Only need
			 * to send value if it is not the own id if (currentComponent !=
			 * vertex.getValue().get()) { // vertex.setValue(new
			 * LongWritable(currentComponent)); for (Edge<LongWritable,
			 * LongWritable> edge : vertex.getEdges()) { LongWritable neighbor =
			 * edge.getTargetVertexId(); if (neighbor.get() > currentComponent)
			 * { sendMessage(neighbor, vertex.getValue()); } } }
			 * vertex.voteToHalt(); return;
			 */
			sendMessageToAllEdges(vertex, vertex.getValue());
		} else {
			boolean changed = false;
			long currentComponent = vertex.getValue().get();
			// did we get a smaller id ?
			for (LongWritable message : messages) {
				long candidateComponent = message.get();
				if (candidateComponent < currentComponent) {
					currentComponent = candidateComponent;
					changed = true;
				}
			}
			// propagate new component id to the neighbors
			if (changed) {
				vertex.setValue(new LongWritable(currentComponent));
				sendMessageToAllEdges(vertex, vertex.getValue());
			}
		}
		vertex.voteToHalt();
	}
}
