package iitp.graph.seq.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
/*
 * Calculate Max & Min degree from AdjList format input graph.
 * 
 * */
public class GraphDegreeInfo {

	public void MinMaxDegree(String inputFile) throws IOException {
		FileReader filereader=new FileReader(inputFile);
		BufferedReader buffer=new BufferedReader(filereader);
		String line="";
		long min=Long.MAX_VALUE;
		long max=0;
		System.out.println("Dataset:"+inputFile);
		System.out.println("Min Initially:"+min);
		System.out.println("Max Initially:"+max);
		while((line=buffer.readLine())!=null)
		{
			String []AdjList=line.split("\t");
			String []edges=AdjList[1].split(",");
			
			if(min>edges.length)
				min=edges.length;
			if(max<edges.length)
				max=edges.length;
			
		}
		System.out.println("Min Final:"+min);
		System.out.println("Max Final:"+max);
		buffer.close();
		filereader.close();
	}
	
	public void inputFileCalls() throws IOException
	{
		String inputGraphfiles[] = { "DBLP-AdjList.txt","Email-Eron-AdjList.txt","roadTX-AdjList.txt","amazonAdjList.txt","internetTopo-AdjList.txt","Youtube-AdjList.txt"};
		for (String inputfiles : inputGraphfiles) {
			MinMaxDegree("/home/nishant/inputGraph/AdjList/"+inputfiles);
		}
	}
	
	public static void main(String args[]) throws IOException {
		
		new GraphDegreeInfo().inputFileCalls();
	}
}
