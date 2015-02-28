package iitp.graph.seq.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

class Vertex{
	public String id;
	public List<Edge> adjacencies;
	public String value;
	
	public String getValue() {
		return value;
	}
	
	public void setValue(String value)
	{
		this.value=value;
	}
	public Vertex(String argName,String argValue)
	{
		this.id=argName;
		this.value=argValue;
		adjacencies= new LinkedList<Edge>();
	}
	
	
}
class Edge{
	public Vertex target;
	public Edge(Vertex argTarget)
	{
		this.target=argTarget;
	}
	public Vertex getTarget()
	{
		return target;
	}
}
class Graph
{
	private Map<String,Vertex> graph;
	Graph()
	{
		graph=new LinkedHashMap<String,Vertex>();
	}
	public void createGraph(String inputPath) throws IOException
	{
		FileReader file_reader = new FileReader(inputPath);
		BufferedReader br =new BufferedReader(file_reader);
		String line;
		
		while((line=br.readLine())!=null)
		{
			StringTokenizer tokenizer = new StringTokenizer(line,"\t");
			String source=tokenizer.nextToken();
			String destination=tokenizer.nextToken();
			StringTokenizer edgeToken=new StringTokenizer(destination,",");
			while(edgeToken.hasMoreTokens())
			addEdge(source,edgeToken.nextToken());
		}
		br.close();
		System.out.println("Graph Created.");
	}
	public void addColor(String inputPath) throws IOException
	{
		FileReader file_reader = new FileReader(inputPath);
		BufferedReader br =new BufferedReader(file_reader);
		String line;
		while((line=br.readLine())!=null)
		{
			StringTokenizer tokenizer = new StringTokenizer(line,"\t");
			String source=tokenizer.nextToken();
			String color=tokenizer.nextToken();
			//while(edgeToken.hasMoreTokens())
			Vertex v=getVertex(source);
			v.setValue(color);
		}
			br.close();
			System.out.println("Color Added.");
	}
	private void addEdge(String source, String destination) 
	{
		// TODO Auto-generated method stub
		Vertex s=getVertex(source);
		Vertex d=getVertex(destination);
		s.adjacencies.add(new Edge(d));
	}
	public Vertex getVertex(String node)
	{
		Vertex v=(Vertex) graph.get(node);
		if(v==null)
		{
			v=new Vertex(node,"0");
			graph.put(node, v);
		}
		return v;
	}
	
	public void validate()
	{
	//	graph.keySet();
		for (Map.Entry<String, Vertex> entry : graph.entrySet())
		{
		   // System.out.println(entry.getKey() + "/" + entry.getValue());
		    Vertex v=entry.getValue();
		    String mycolor=v.getValue();
		    Edge e;
		    for(int i=0;i<v.adjacencies.size();i++)
		    {
		    	e=v.adjacencies.get(i);
		    	if(mycolor.equals(e.getTarget().value))
		    	{
		    		System.out.println("Problem!!!");
		    	}
		    }
		}
		System.out.println("Validation Finish.");
	}
	
}

public class ValidateProperGraphColor {

	public static void main(String args[]) throws IOException {
		Graph g=new Graph();
		g.createGraph("/home/nishant/inputGraph/AdjList/Youtube-AdjList.txt");
		g.addColor("/home/nishant/GiraphOutput/LLDF/Youtube-AdjList.txt/part-m-00000");
		
		//g.createGraph("/home/nishant/inputGraph/testGraph4.txt");
	//	g.addColor("/home/nishant/GiraphOutput/LIDO/testGraph9dis.txt/part-m-00000");
		g.validate();
	}
}
