import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class GiraphRunner implements Tool {
	static 
	{
	    Configuration.addDefaultResource("giraph-site.xml");
	}
	private Configuration conf;
	private static final Logger LOG = Logger.getLogger(GiraphRunner.class);
	
	
	
	public static void main(String[] args) throws Exception {
	    System.exit(ToolRunner.run(new GiraphRunner(), args));
	  }

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf=conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(null==getConf())
		{
			conf=new Configuration();
		}
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		CommandLine cmd = ConfigurationUtils.parseArgs(giraphConf, args);
	    if (null == cmd) {
	      return 0; // user requested help/info printout, don't run a job.
	    }
	    final String vertexClassName = args[0];
	    final String jobName = "Giraph: " + vertexClassName;
	    GiraphJob job = new GiraphJob(giraphConf, jobName);
	    prepareHadoopMRJob(job, cmd);
	    if (LOG.isDebugEnabled()) 
	    {
	        LOG.debug("Attempting to run Vertex: " + vertexClassName);
	    }
	    boolean verbose = !cmd.hasOption('q');
	    return job.run(verbose) ? 0 : -1;
	    
		
	}

	private void prepareHadoopMRJob(GiraphJob job, CommandLine cmd) throws URISyntaxException {
		// TODO Auto-generated method stub
		if (cmd.hasOption("vof") || cmd.hasOption("eof")) 
		{
		      if (cmd.hasOption("op")) 
		      {
		        FileOutputFormat.setOutputPath(job.getInternalJob(),new Path(cmd.getOptionValue("op")));
		      }
		}
		if (cmd.hasOption("cf")) 
		{
		      DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")),
		          job.getConfiguration());
		}
	}
}
