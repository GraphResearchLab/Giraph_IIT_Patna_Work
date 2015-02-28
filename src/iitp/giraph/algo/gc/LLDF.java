package iitp.giraph.algo.gc;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LLDF extends
		BasicComputation<LongWritable, LongWritable, LongWritable, Text> {
	@Override
	public void compute(
			Vertex<LongWritable, LongWritable, LongWritable> vertex,
			Iterable<Text> msg) throws IOException {
		boolean MaxVal = true;
		Text OutMsg = new Text();
		String temp = "";
		if (getSuperstep() == 0) {
			vertex.setValue(new LongWritable(-1));
			temp = vertex.getId().toString() + "_" + vertex.getNumEdges();
			OutMsg.set(temp);
			/*
			 * for(Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
			 * sendMessage(edge.getTargetVertexId(),OutMsg); }
			 */
			sendMessageToAllEdges(vertex, OutMsg);
		} else {
			if (vertex.getValue().get() == -1) {
				for (Text message : msg) {
					String[] income = message.toString().split("_");
					if (vertex.getNumEdges() < Long.parseLong(income[1])) {
						MaxVal = false;
					}
					if (vertex.getNumEdges() == Long.parseLong(income[1])) {
						if (Long.parseLong(income[0]) > vertex.getId().get())
							MaxVal = false;
					}
				}
				if (MaxVal == true) {
					vertex.setValue(new LongWritable(getSuperstep()));
					// System.out.println("Colored:"+vertex.getId().toString()+" With Superstep:"+getSuperstep());
				} else {
					temp = vertex.getId().toString() + "_"
							+ vertex.getNumEdges();
					OutMsg.set(temp);
					/*
					 * for(Edge<LongWritable, LongWritable> edge :
					 * vertex.getEdges()) {
					 * sendMessage(edge.getTargetVertexId(),OutMsg); }
					 */
					sendMessageToAllEdges(vertex, OutMsg);
				}

			}
		}

		vertex.voteToHalt();
	}
}
