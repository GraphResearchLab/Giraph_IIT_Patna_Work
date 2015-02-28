package iitp.graph.seq.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
/*
 * Find maximum and minimum color id from IdWithValueVertexOutputFormat result file
 * 
 * */
public class GraphColorInfo {

	public void generateMatrix(String input) throws IOException
	{
		String inputFile="/home/nishant/GiraphOutput/LLDF/"+input+"/part-m-00000";
		FileReader filereader=new FileReader(inputFile);
		BufferedReader buffer=new BufferedReader(filereader);
		String line="";
		long min=Long.MAX_VALUE;
		long max=0;
		HashSet<String> colorset=new HashSet<String>();
		System.out.println("Dataset:"+input);
		//System.out.println("Min Initially:"+min);
		//System.out.println("Max Initially:"+max);
		while((line=buffer.readLine())!=null)
		{
			String []AdjList=line.split("\t");
			//String []edges=AdjList[1].split(",");
			colorset.add(AdjList[1]);
			
			if(min>Long.parseLong(AdjList[1]))
				min=Long.parseLong(AdjList[1]);
			if(max<Long.parseLong(AdjList[1]))
				max=Long.parseLong(AdjList[1]);
		}
		System.out.println("Min Color:"+min);
		System.out.println("Max Color:"+max);
		System.out.println("Total Color Used:"+colorset.size());
		System.out.println();
		colorset.clear();
		buffer.close();
		filereader.close();
		
		
		
	}
	public void fileCalls() throws IOException
	{
		String inputGraphfiles[] = { "internetTopo-AdjList.txt",
				"amazonAdjList.txt", "DBLP-AdjList.txt",
				"Email-Eron-AdjList.txt", "roadTX-AdjList.txt",
				"Youtube-AdjList.txt" };
		for (String inputfiles : inputGraphfiles) {
			try {
				generateMatrix(inputfiles);
			} catch (Exception e) {
				
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String args[]) throws IOException
	{
		new GraphColorInfo().fileCalls();
	}
}
