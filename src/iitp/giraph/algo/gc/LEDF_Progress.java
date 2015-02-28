package iitp.giraph.algo.gc;

import java.io.IOException;
import java.util.HashSet;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LEDF_Progress extends
		BasicComputation<LongWritable, LongWritable, LongWritable, Text> {
	public static HashSet<Long> neighborList=new HashSet<Long>();
	public static long satDegree=0;
	@Override
	public void compute(
			Vertex<LongWritable, LongWritable, LongWritable> vertex,
			Iterable<Text> msg) throws IOException {
		// TODO Auto-generated method stub
		Text OutMsg = new Text();
		boolean color = true;
		String temp = "";
		if (getSuperstep() == 0) 
		{
			vertex.setValue(new LongWritable(-1));
			for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
				temp = "COLOR_"+satDegree + "_" + vertex.getNumEdges()+"_"+vertex.getId().toString();
				OutMsg.set(temp);
				sendMessage(edge.getTargetVertexId(), OutMsg);
			}
		} 
		else 
		{
			if (vertex.getValue().get() == -1) 
			{
				
				for(Text message :msg )
				{
					String[] income = message.toString().split("_");
					String msgPattern=income[0];
				//	String saturationDegreeDest=null;
				//	String vertexDegreeDest=null;
					String neighborColor=null;
					if(msgPattern.equals("SAT"))
					{
						neighborColor=income[1];
						neighborList.add(Long.parseLong(neighborColor));
						satDegree=neighborList.size();
					}
				}
				for (Text message : msg) 
				{
					String[] income = message.toString().split("_");
					String msgPattern=income[0];
					String saturationDegreeDest=null;
					String vertexDegreeDest=null;
					String vertexIdDest=null;
					//String neighborColor=null;
					if(msgPattern.equals("COLOR"))
					{
						saturationDegreeDest=income[1];
						vertexDegreeDest=income[2];
						vertexIdDest=income[3];
						if (satDegree < Long.parseLong(saturationDegreeDest)) 
						{
							color = false;
						}
						if (satDegree == Long.parseLong(saturationDegreeDest)) 
						{
							if(Long.parseLong(vertexDegreeDest) == vertex.getNumEdges())
							{
								if(vertex.getId().get() < Long.parseLong(vertexIdDest))
									color=false;
							}
							else if (Long.parseLong(vertexDegreeDest) > vertex.getNumEdges())
								color = false;
						}	
						if (color == true) 
						{
							vertex.setValue(new LongWritable(getSuperstep()));
							for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
								temp = "SAT_"+vertex.getValue();
								OutMsg.set(temp);
								sendMessage(edge.getTargetVertexId(), OutMsg);
							}
							System.out.println("Colored:" + vertex.getId().toString()+ " With Superstep:" + getSuperstep());
						} 
						else 
						{
							for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) 
							{
								temp = "COLOR_"+satDegree + "_" + vertex.getNumEdges()+"_"+vertex.getId().toString();
								OutMsg.set(temp);
								sendMessage(edge.getTargetVertexId(), OutMsg);
							}
						}
					}
				}
			}
			vertex.voteToHalt();
		}
	}
}