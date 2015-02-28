package iitp.giraph.io;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.python.google.common.collect.Lists;
/*
 * File with input pattern as follow
 * VertexId <tab> EdgeList_Saperated_By_","
 * LongWritable <tab> LongWritable,LongWritable,....
 */

public class LongLongAdjListFileInputFormat extends
		TextVertexInputFormat<LongWritable, LongWritable, LongWritable> {

	private ImmutableClassesGiraphConfiguration<LongWritable, LongWritable, LongWritable> conf;

	public void setConf(
			ImmutableClassesGiraphConfiguration<LongWritable, LongWritable, LongWritable> configuration) {
		this.conf = configuration;
	}

	public ImmutableClassesGiraphConfiguration<LongWritable, LongWritable, LongWritable> getConf() {
		return conf;
	}

	@Override
	public TextVertexInputFormat<LongWritable, LongWritable, LongWritable>.TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {

		return new LongLongLongLongVertexReader();
	}

	public class LongLongLongLongVertexReader
			extends
			TextVertexInputFormat<LongWritable, LongWritable, LongWritable>.TextVertexReader {
		private final Pattern saperator = Pattern.compile("[\t ]");

		@Override
		public Vertex<LongWritable, LongWritable, LongWritable> getCurrentVertex()
				throws IOException, InterruptedException {
			Vertex<LongWritable, LongWritable, LongWritable> vertex = conf.createVertex();
			String tokens[] = saperator.split(getRecordReader().getCurrentValue().toString());
			List<Edge<LongWritable, LongWritable>> edges = Lists.newArrayListWithCapacity(tokens.length - 1);
			//long weight = ((long) 1.0) / (tokens.length - 1);
			LongWritable VertexId = new LongWritable(Long.parseLong(tokens[0]));
			//LongWritable VertexValue = new LongWritable(
			//		Long.parseLong(tokens[1]));
			//System.out.println(VertexValue);
			String edgeId[] = tokens[1].split(",");
			for (String edge : edgeId) {
				edges.add(EdgeFactory.create(new LongWritable(Long.parseLong(edge)),new LongWritable(1)));
			}
			vertex.initialize(VertexId,new LongWritable(10),edges);
			return vertex;
		}
		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

	}

}