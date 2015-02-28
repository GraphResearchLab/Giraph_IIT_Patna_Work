package iitp.graph.seq.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class ConnectedComponentCount {
	
	public void countComponents(String inputFile) throws IOException
	{
		FileReader filereader=new FileReader(inputFile);
		BufferedReader buffer=new BufferedReader(filereader);
		String line="";
		/*long min=Long.MAX_VALUE;
		long max=0;*/
		
		System.out.println("Dataset:"+inputFile);
		//System.out.println("Min Initially:"+min);
		//System.out.println("Max Initially:"+max);
		HashSet<String> set=new HashSet<String>();
		HashMap<String,Long> table=new HashMap<String,Long>();
		table.clear();
		set.clear();
		
		while((line=buffer.readLine())!=null)
		{
			String []AdjList=line.split("\t");
			//String []edges=AdjList[1].split(",");
			
			/*if(min>Long.parseLong(AdjList[1]))
				min=Long.parseLong(AdjList[1]);
			if(max<Long.parseLong(AdjList[1]))
				max=Long.parseLong(AdjList[1]);*/
			if(table.containsKey(AdjList[1]))
			{
				long temp=table.get(AdjList[1]);
				temp++;
				table.put(AdjList[1], temp);
			}
			else
			{
				table.put(AdjList[1],1l);
			}
			set.add(AdjList[1]);	
		}
		/*System.out.println("Min Color:"+min);
		System.out.println("Max Color:"+max);*/
		buffer.close();
		filereader.close();
		
		System.out.println("Component Count:"+table.size());
		for(String items:table.keySet())
		{
			System.out.println(items+":"+table.get(items));
		}
	}
	public void callFunction()
	{
		String inputGraphfiles[] = {  "DBLP-AdjList.txt","Email-Eron-AdjList.txt","roadTX-AdjList.txt"
				
				 };
		for (String inputfiles : inputGraphfiles) {
			try {
				countComponents("/home/nishant/GiraphOutput/ConnectedComponents/"+inputfiles+"/part-m-00000");
			} catch (Exception e) {
				
				e.printStackTrace();
			}
		}
		
		
	}
	
	public static void main(String args[])
	{
		new ConnectedComponentCount().callFunction();
	}
}
