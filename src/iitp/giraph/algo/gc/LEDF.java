package iitp.giraph.algo.gc;

import java.io.IOException;
import java.util.HashSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LEDF extends
		BasicComputation<LongWritable, LongWritable, LongWritable, Text> {
	public static String delimiter = "_";
	public static HashSet<Long> neighborList = new HashSet<Long>();
	public static long myselectionToken;

	public long getMySelectionToken() {
		return myselectionToken;

	}

	public void updateMySelectionToken(long token) {
		neighborList.add(token);
		myselectionToken = neighborList.size();
	}
	
	@Override
	public void compute(
			Vertex<LongWritable, LongWritable, LongWritable> vertex,
			Iterable<Text> msg) throws IOException {
		Text selectionMsg = new Text();
		Text saturationMsg = new Text();
		boolean colorMe = true;
		String temp = "";
		// send degree for selection
		if (getSuperstep() == 0) {
			
			vertex.setValue(new LongWritable(-1));
			myselectionToken = vertex.getNumEdges();
			updateMySelectionToken(-1);
			temp = vertex.getNumEdges() + delimiter + vertex.getNumEdges()
					+ delimiter + vertex.getId().get();
			selectionMsg.set(temp);
			
			sendMessageToAllEdges(vertex, selectionMsg);
		}
		// check and get colored. send saturation msg to calculate saturation
		// degree
		else if (getSuperstep() % 2 == 1) {
		
			if (vertex.getValue().get() == -1) {
				for (Text message : msg) {
					String[] inMsg = message.toString().split(delimiter);
					long targetselectionToken = Long.parseLong(inMsg[0]);
					long targetDegree = Long.parseLong(inMsg[1]);
					long targetId = Long.parseLong(inMsg[2]);
					

					if (targetselectionToken > getMySelectionToken()) {
						colorMe = false;
					} else if (targetselectionToken == getMySelectionToken()) {
						if (targetDegree > vertex.getNumEdges())
							colorMe = false;
						else if (targetDegree == vertex.getNumEdges()) {
							if (targetId > vertex.getId().get())
								colorMe = false;
						}
					}
				}
				if (colorMe == true) {
					// System.out.println("Coloring vertex:"+vertex.getId().get()+" With:"+((int)getSuperstep()/2));
					vertex.setValue(new LongWritable(((int) getSuperstep() / 2)));
					temp = vertex.getValue().get() + "";
					saturationMsg.set(temp);
					neighborList.clear();
					sendMessageToAllEdges(vertex, saturationMsg);
				} else
					sendMessageToAllEdges(vertex, new Text("-1"));
			}
		}
		// calculate new saturation degree and send it as selection msg to all
		// neighbor
		else {
			
			if (vertex.getValue().get() == -1) {
				for (Text message : msg) {
					String[] inMsg = message.toString().split(delimiter);
						long color = Long.parseLong(inMsg[0]);
						updateMySelectionToken(color);
						temp = getMySelectionToken() + delimiter
								+ vertex.getNumEdges() + delimiter
								+ vertex.getId().get();
						selectionMsg.set(temp);
						sendMessageToAllEdges(vertex, selectionMsg);
				}
				
			}

		}
		vertex.voteToHalt();
	}
}
