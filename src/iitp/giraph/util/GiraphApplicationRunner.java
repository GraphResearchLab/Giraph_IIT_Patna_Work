package iitp.giraph.util;

import iitp.giraph.algo.gc.*;
import iitp.giraph.algo.utils.ConnectedComponents;
import iitp.giraph.algo.utils.IndegreeOfvertex;
import roughTest.EdgeTest;
import iitp.giraph.io.LongLongAdjListFileInputFormat;
import iitp.giraph.io.LongLongEdgeFileInputFormat;
import iitp.giraph.io.LongLongVertexIdValueFileOutputFormat;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.examples.ConnectedComponentsComputation;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GiraphApplicationRunner implements Tool 
{

	private Configuration conf;
	private static String inputPath;
	private static String outputPath;
	
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	
	
	@Override
	public Configuration getConf() 
	{
		return conf;
	}
	@Override
	public void setConf(Configuration conf) 
	{
		this.conf=conf;
	}
	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception 
	{
		String inputGraphfiles[]={"DBLP-AdjList.txt","Email-Eron-AdjList.txt","roadTX-AdjList.txt","amazonAdjList.txt","internetTopo-AdjList.txt","Youtube-AdjList.txt"};
		
	//	for(String inputfiles:inputGraphfiles)
		{
	//	setInputPath("/home/nishant/inputGraph/AdjList/"+inputfiles);
			
			String	inputfiles="Youtube-AdjList.txt";
			setInputPath("/home/nishant/inputGraph/AdjList/"+inputfiles);
			
			//String	inputfiles="testGraph4.txt";
			//setInputPath("/home/nishant/inputGraph/"+inputfiles);
			GiraphConfiguration giraphConf=new GiraphConfiguration(getConf());
			giraphConf.setComputationClass(LIDO.class);
		//giraphConf.setEdgeInputFormatClass(IntNullTextEdgeInputFormat.class);
			giraphConf.setVertexInputFormatClass(LongLongAdjListFileInputFormat.class);
			giraphConf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
			GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(getInputPath()));
		
	//GiraphFileInputFormat.addEdgeInputPath(giraphConf,new Path(getInputPath()));
	//	GiraphFileInputFormat.setEdgeInputPath(giraphConf, new Path(getInputPath()));
			giraphConf.setWorkerConfiguration(0, 1, 100.0f);
			giraphConf.SPLIT_MASTER_WORKER.set(giraphConf, false);
			giraphConf.setLocalTestMode(true);
			giraphConf.setMaxNumberOfSupersteps(10000);
			giraphConf.USE_OUT_OF_CORE_GRAPH.set(giraphConf, true);
			
			GiraphJob job =new GiraphJob(giraphConf,getClass().getName());
		
			setOutputPath("/home/nishant/GiraphOutput/"+giraphConf.getComputationName()+"/"+inputfiles);
			FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(getOutputPath()));
		//FileInputFormat.setInputPaths(job, new Path(getInputPath()));
			File directory=new File(getOutputPath());
			FileUtils.deleteDirectory(directory);
			job.run(true);
		}
		return 1;
	}
	
	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new GiraphApplicationRunner(),args);
		Process p = Runtime.getRuntime().exec("gedit "+outputPath+"/part-m-00000");
	}
}
