package iitp.giraph.algo.gc;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LSLDF extends
		BasicComputation<LongWritable, LongWritable, LongWritable, Text> {

	public void compute(
			Vertex<LongWritable, LongWritable, LongWritable> vertex,
			Iterable<Text> msg) throws IOException {
		// TODO Auto-generated method stub
		boolean maxDegreeVertex = true;
		boolean minDegreeVertex = true;
		Text OutMsg = new Text();
		String temp = "";

		if (getSuperstep() == 0) {
			vertex.setValue(new LongWritable(-1));
			/*
			 * for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
			 * temp = vertex.getId().toString() + "_" + vertex.getNumEdges();
			 * OutMsg.set(temp); sendMessage(edge.getTargetVertexId(), OutMsg);
			 * }
			 */
			temp = vertex.getId().toString() + "_" + vertex.getNumEdges();
			OutMsg.set(temp);
			sendMessageToAllEdges(vertex, OutMsg);

		} else {
			if (vertex.getValue().get() == -1) {
				for (Text message : msg) {
					String[] inComingMsg = message.toString().split("_");
					if (vertex.getNumEdges() < Long.parseLong(inComingMsg[1])) {
						maxDegreeVertex = false;
					}
					if (vertex.getNumEdges() > Long.parseLong(inComingMsg[1])) {
						minDegreeVertex = false;
					}
					if (vertex.getNumEdges() == Long.parseLong(inComingMsg[1])) {
						if (Long.parseLong(inComingMsg[0]) > vertex.getId()
								.get()) {
							maxDegreeVertex = false;
							minDegreeVertex = false;
						}
					}
				}
				if (maxDegreeVertex == true) {
					vertex.setValue(new LongWritable(2 * getSuperstep()));
				} else if (minDegreeVertex == true) {
					vertex.setValue(new LongWritable(2 * getSuperstep() - 1));
				} else {
					/*
					 * for (Edge<LongWritable, LongWritable> edge : vertex
					 * .getEdges()) { temp = vertex.getId().toString() + "_" +
					 * vertex.getNumEdges(); OutMsg.set(temp);
					 * sendMessage(edge.getTargetVertexId(), OutMsg); }
					 */
					temp = vertex.getId().toString() + "_"
							+ vertex.getNumEdges();
					OutMsg.set(temp);
					sendMessageToAllEdges(vertex, OutMsg);
				}

			}
		}

		vertex.voteToHalt();

	}

}
