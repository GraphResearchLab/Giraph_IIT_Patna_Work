package iitp.giraph.io;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;

/*
 * 
 * Edge input format: Source_vertex [\t] Destination_vertex
 * */

public class LongLongEdgeFileInputFormat extends TextEdgeInputFormat<LongWritable, NullWritable> {
/** Splitter for endpoints */
private static final Pattern SEPARATOR = Pattern.compile("[\t]");

@Override
public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split,TaskAttemptContext context) throws IOException {
	
	return new LongLongTextEdgeReader();
}

	public class LongLongTextEdgeReader extends TextEdgeReaderFromEachLine 
	{

		@Override
		protected LongWritable getSourceVertexId(Text line) throws IOException {
			String[] splits = SEPARATOR.split(line.toString());
			
			if (splits.length < 2) {
			throw new IOException("unable to get source vertex Id from line '" + line + "'");
			}
			return new LongWritable(Long.parseLong(splits[0]));
		}

		@Override
		protected LongWritable getTargetVertexId(Text line) throws IOException {
			String[] splits = SEPARATOR.split(line.toString());
			
			if (splits.length < 2) {
			throw new IOException("unable to get target vertex Id from line '" + line + "'");
			}
			return new LongWritable(Long.parseLong(splits[1]));
		}

		@Override
		protected NullWritable getValue(Text line) throws IOException {
		
			return NullWritable.get();
		}

	
	}
	
}
