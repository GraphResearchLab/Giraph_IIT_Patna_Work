package iitp.giraph.algo.gc;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LIDO extends
		BasicComputation<LongWritable, LongWritable, LongWritable, Text> {
	public static String delimiter = "_";
	public static long incedentDegree[]=new long[1500000];// incedent degree is shared between all vertices !!!! o:O)

	public long getMySelectionToken(int index) {
		return incedentDegree[index];
	}

	public void setMySelectionToken(long token,int index) {
		incedentDegree[index] = token;
	}

	public void updateMySelectionToken(int index) {
		incedentDegree[index]++;
	}

	@Override
	public void compute(
			Vertex<LongWritable, LongWritable, LongWritable> vertex,Iterable<Text> msg) throws IOException 
	{
		
		Text selectionMsg = new Text();
		Text saturationMsg = new Text();
		boolean colorMe = true;
		String temp = "";
		System.out.println("Superstep:" +getSuperstep());
		if (getSuperstep() == 0) 
		{
			vertex.setValue(new LongWritable(-1));
			System.out.println("vertex value:"+vertex.getValue());
			setMySelectionToken(0,(int)vertex.getId().get());
			temp = getMySelectionToken((int)vertex.getId().get()) + delimiter + vertex.getNumEdges()
					+ delimiter + vertex.getId().get();
			selectionMsg.set(temp);
			System.out.println(vertex.getId()+" sending Msg to neighbour: "+temp);
			sendMessageToAllEdges(vertex, selectionMsg);
		} 
		else if (getSuperstep() % 2 == 1) 
		{
			if (vertex.getValue().get() == -1) 
			{
				for (Text message : msg) 
				{
					String[] inMsg = message.toString().split(delimiter);
					long targetselectionToken = Long.parseLong(inMsg[0]);
					long targetDegree = Long.parseLong(inMsg[1]);
					long targetId = Long.parseLong(inMsg[2]);

					if (targetselectionToken > getMySelectionToken((int)vertex.getId().get())) 
					{
						colorMe = false;
					} 
					else if (targetselectionToken == getMySelectionToken((int)vertex.getId().get())) 
					{
						if (targetDegree > vertex.getNumEdges())
							colorMe = false;
						else if (targetDegree == vertex.getNumEdges()) 
						{
							if (targetId > vertex.getId().get())
								colorMe = false;
						}
					}
				}
				if (colorMe == true) 
				{
					// System.out.println("Coloring vertex:"+vertex.getId().get()+" With:"+((int)getSuperstep()/2));
					vertex.setValue(new LongWritable((getSuperstep())));
					System.out.println(vertex.getId()+" is getting colored:"+ getSuperstep());
					temp = vertex.getValue().get() + "";
					saturationMsg.set(temp);
					sendMessageToAllEdges(vertex, saturationMsg);
					System.out.println(vertex.getId()+" sending Msg to neighbour: "+temp);
				}
			}
			
		} 
		else 
		{
			if (vertex.getValue().get() == -1) 
			{
				for (Text message : msg) 
				{
					String[] inMsg = message.toString().split(delimiter);
					long color = Long.parseLong(inMsg[0]);
					if (color > -1)
					{
						updateMySelectionToken((int)vertex.getId().get());
						System.out.println(vertex.getId()+" updating token to: "+getMySelectionToken((int)vertex.getId().get()));
					}
				}
				temp = getMySelectionToken((int)vertex.getId().get()) + delimiter
						+ vertex.getNumEdges() + delimiter
						+ vertex.getId().get();
				selectionMsg.set(temp);
				System.out.println(vertex.getId()+" sending Msg to neighbour: "+temp);
				sendMessageToAllEdges(vertex, selectionMsg);
			}
			vertex.voteToHalt();

		}
		
	}

}
