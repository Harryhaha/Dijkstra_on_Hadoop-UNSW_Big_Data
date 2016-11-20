package comp9313.ass4;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SingleSourceSP {
	public static enum MY_COUNTER {
		DISTANCE_UPDATE
	};

	
	// Job2: run the Dijkstra algorithm iteratively using mapreduce 
	/*
	 * The data structure:
	 * srcNode	dist	N/D|destNode1:dist1 destNode2:dist2 destNode3:dist3 .........
	 * the line on the left side of "|" is separated by "\t", while the right side is separated by "\s"
	 * status flag "N" means it is updated in the reducer at last round , while "D" means it is unchanged
	 * dist is set to "-1" if it is infinite from the source node currently
	 */
	public static class SPMapper extends Mapper<Object, Text, LongWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Get rid of empty line(s)
			if (value.toString().trim().equals("")){
				return;
			}
			
			String[] strings = value.toString().split("\\|");
			String[] fromNodeIdAndSrcDistAndDupFlag = strings[0].split("\t");

			String fromNodeId = fromNodeIdAndSrcDistAndDupFlag[0];
			Integer fromNodeIdInt = Integer.parseInt(fromNodeId);
			String srcDist = fromNodeIdAndSrcDistAndDupFlag[1];
			Double srcDistDouble = Double.parseDouble(srcDist);
			String dupFlag = fromNodeIdAndSrcDistAndDupFlag[2];

			// The adjacency list could be empty during the iterations
			// for the node that does not has outgoing edges before current iteration
			String[] toNodeIdsAndDist = {};
			if (strings.length != 1) {  
				toNodeIdsAndDist = strings[1].split(" ");
				// Emit the adjacency list to reverse/pass the graph structure, this
				// emit should be for all mapper inputs
				context.write(new LongWritable(fromNodeIdInt), new Text(srcDist+"\t"+dupFlag+"|"+strings[1]));
			} else {
				// If the adjacency list is empty, leave the right side of '|' as empty
				context.write(new LongWritable(fromNodeIdInt), new Text(srcDist+"\t"+dupFlag+"|"));
			}

			// If the srcDist is not -1(infinite), then process it otherwise just ignore it.
			if (srcDistDouble != -1.0 /* && toNodeIdsAndDist.length != 0 */) {
				// If it is same as last iteration, then just return to avoid duplicate computation
				if (dupFlag.equals("D")){
					return;
				}

				// Emit all related (destNodeId, updatedistForDestNode) pairs only if the srcDist is not infinite
				for (int i = 0; i < toNodeIdsAndDist.length; i++) {
					String[] fromNodeIdAndToNodeId = toNodeIdsAndDist[i].split(":");
					String toNodeId = fromNodeIdAndToNodeId[0];
					Integer toNodeIdInt = Integer.parseInt(toNodeId);

					String srcDestDist = fromNodeIdAndToNodeId[1];
					Double srcDestDistDouble = Double.parseDouble(srcDestDist);

					// Update the dist by addition of the dist of the source node 
					// and the dist between current node and the source node
					Double updatedDist = srcDistDouble + srcDestDistDouble;
					String updatedDistStr = String.valueOf(updatedDist);

					context.write(new LongWritable(toNodeIdInt), new Text(updatedDistStr));
				}
			}
		}
	}

	
	public static class SPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String origAdjacencyList = "";
			// Accumulate all the distances associated with this key node to get the minimal one
			// and replace the minimal dist with the original one if minimal dist does exist
			ArrayList<Double> updatedDistances = new ArrayList<Double>();
			for (Text updatedDistOrAdjList : values) {
				String updatedDistOrAdjListStr = updatedDistOrAdjList.toString();
				if (updatedDistOrAdjListStr.indexOf('|') == -1) {
					// Contains no "|" means it is one of the updated distances
					Double distance = Double.parseDouble(updatedDistOrAdjListStr);
					updatedDistances.add(distance);
				} else {
					// Contains "|" means it is original adjacency list
					origAdjacencyList = updatedDistOrAdjListStr;
				}
			}

			// Split the whole string here first to be used in multiple places to improve efficiency.
			String[] strings = origAdjacencyList.split("\\|");
			String origSrcDistStr = "";

			Double currentDistance = -1.0;
			// Just in case the original adjacency list is empty
			if (!origAdjacencyList.equals("")) {
				origSrcDistStr = strings[0].split("\\t")[0];
				currentDistance = Double.parseDouble(origSrcDistStr);
			}

			// Get the minimal distance if it exists among all the 
			// updated distances obtained from the mapper
			Double minDistance = Double.POSITIVE_INFINITY;
			for (int i = 0; i < updatedDistances.size(); i++) {
				if (updatedDistances.get(i) < minDistance) {
					minDistance = updatedDistances.get(i);
				}
			}

			// If the updatedMinDistance is smaller than currentMinDistance, than update the whole adjacency list string
			if (minDistance != Double.POSITIVE_INFINITY && (currentDistance == -1.0 || minDistance < currentDistance)) {
				String minDistanceStr = String.valueOf(minDistance);
				// Just in case that origAdjacencyList is "" or "dist|"
				if (strings.length >= 2) {
					origAdjacencyList = minDistanceStr + "\t" + "N" + "|" + strings[1];
				} else {
					origAdjacencyList = minDistanceStr + "\t" + "N" + "|";
				}

				// Update(increment) the counter to indicate the program cannot stop 
				// after this round mapreduce since it is still get updated
				context.getCounter(MY_COUNTER.DISTANCE_UPDATE).increment(1);
			} else {
				// otherwise keep the original info for this source node but set the status flag 
				// to be Duplicated indicate the next round mapper does not need to update the 
				// dist for linking nodes of this source node again, to improve the efficiency
				if (strings.length >= 2) {
					origAdjacencyList = origSrcDistStr + "\t" + "D" + "|" + strings[1];
				} else {
					origAdjacencyList = origSrcDistStr + "\t" + "D" + "|";
				}
			}

			context.write(key, new Text(origAdjacencyList));
		}
	}
	
	

	
	// Job1: Convert the input file to the desired data format file for further iteration
	/*
	 * The data structure:
	 * srcNode	dist	N/D|destNode1:dist1 destNode2:dist2 destNode3:dist3 .........
	 * the line on the left side of "|" is separated by "\t", while the right side is separated by "\s"
	 * status flag "N" means it is updated in the reducer at last round , while "D" means it is unchanged
	 * dist is set to "-1" if it is infinite from the source node currently
	 */
	public static class ConvertDateFormatMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strings = value.toString().split(" ");
			String fromNodeId = strings[1];
			String toNodeIdAndDist = strings[2] + ":" + strings[3];
			
			context.write(new Text(fromNodeId), new Text(toNodeIdAndDist));
		}
	}
	public static class ConvertDateFormatReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Get the queryNodeId (done by Configuration get/set) to specify(initialize) the dist 
			// of this node is 0.0, while others are all initialized to be -1.0 (inifinite)
			Configuration conf = context.getConfiguration();
		    String queryNodeId = conf.get("queryNodeId");
		    
		    String distAndAdjList = "";
		    if( key.toString().equals(queryNodeId) ){
		    	distAndAdjList += "0.0\tN|";
		    } else {
		    	distAndAdjList += "-1.0\tN|";
		    }
		    
			for (Text value : values) {
				distAndAdjList += value.toString() + " ";
			}
			distAndAdjList = distAndAdjList.trim();
			
			context.write( key, new Text(distAndAdjList));
		}
	}

	
	
	
	// Job3: write the final result to HDFS
	public static class FinalWriteMapper extends Mapper<Object, Text, LongWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Get rid of empty line(s)
			if (value.toString().trim().equals("")){
				return;
			}
			
			String line = value.toString().split("\\|")[0];
			String[] toNodeIdAndDistArray = line.split("\t");
			Integer toNodeId = Integer.parseInt(toNodeIdAndDistArray[0]);
			String dist = toNodeIdAndDistArray[1];
			// In case the node is unreachable from the source node, then just ignore it
			if( Double.parseDouble(dist) == -1.0 ){
				return;
			}
			
			// Use the "toNodeId" as key to guarantee the targetNodes are sorted in numerical order
			context.write(new LongWritable(toNodeId), new Text(dist));
		}
	}
	public static class FinalWriteReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Get the queryNodeId (done by Configuration get/set) from outside 
			/*
			 * The final output format:
			 * queryNode targetNode1 shortestDist
			 * queryNode targetNode2 shortestDist
			 * ..................................
			 */
			Configuration conf = context.getConfiguration();
		    String queryNodeId = conf.get("queryNodeId");
		    
		    String toNodeId = String.valueOf(key.get());
		    
			for (Text value : values) {
				String dist = value.toString();
				context.write( NullWritable.get(), new Text(queryNodeId+" "+toNodeId+" "+dist));
			}
		}
	}
	
	
	
	
	public static void main(String[] args) throws Exception {        
        String rawInput = args[0];
        
        String OUT = args[1];
        String output = OUT;
        
        // Write the intermediate results to HDFS with the path of output+"/tmp/"+systemtime
        // and this would be used as the path of mapper input at next round
        String outputTmp = OUT + "/tmp/" + System.nanoTime();

        String queryNodeId = args[2];
        
        // Set the real input path for mapreduce after transforming the original data format
        // Here just simply assuming the file type is txt
        String input = rawInput.substring(0, rawInput.length()-4) + ".transform";
        
        // Convert the input file to the desired format for iteration using one mapreduce job
        convertDataFormat(queryNodeId, rawInput, input );
        
		// Change the "Input" to the real file to be read for the Dijkstra algorithm
//        input = input + "/part-r-00000";
//        input = input;
        
        boolean isdone = false;
        while (isdone == false) {
        	Configuration conf2 = new Configuration();
    		Job job2 = Job.getInstance(conf2, "find Single-source shortest path");
    		
    		job2.setJarByClass(SingleSourceSP.class);
    		job2.setMapperClass(SPMapper.class);
    		job2.setReducerClass(SPReducer.class);
    		
    		job2.setOutputKeyClass(LongWritable.class);
    		job2.setOutputValueClass(Text.class);
    		
    		FileInputFormat.addInputPath(job2, new Path(input));
    		FileOutputFormat.setOutputPath(job2, new Path(outputTmp));
    		
    		// Set the number of reducer as 3 for the job running Dijkstra algorithm 
            job2.setNumReduceTasks(3); 
    		
    		job2.waitForCompletion(true); 
        	
    		// Use the current output file as the file to be read by mapper at the next round iteratively...
//    		input = outputTmp + "/part-r-00000";
    		input = outputTmp;
    		
    		// Check for convergence condition if any node is still left then continue else stop  
            // The best way is to use the counter supported by Hadoop MapReduce. This is the recommended way.
            // We can also read the output into memory to check if all nodes get the distances. However, this is not efficient.
            // If the counter is 0, means all dist are already updated to the shortest distance and could stop
            long count = job2.getCounters().findCounter(MY_COUNTER.DISTANCE_UPDATE).getValue();
    		if( count == 0 ){
    			isdone = true;  // Actually no need ....
    			break;
    		}
    		
    		// New output folder for next round
    		outputTmp = OUT + "/tmp/" + System.nanoTime();
        }

        // Extract the final result and write it to the hdfs file, with the filename, e.g. "output+"/NA"+queryId"
        writeFinalResult( queryNodeId, outputTmp, rawInput, output );
        // DONE !
    }
	
	
	
	
	public static void convertDataFormat(String queryNodeId, String rawInput, String input ) throws Exception {
		Configuration conf1 = new Configuration();
		// Passing the queryNodeId to be used in the reducer
        conf1.set("queryNodeId", queryNodeId);
        
        Job job1 = Job.getInstance( conf1, "Convert the input file to the desired format for iteration" );
        job1.setJarByClass(SingleSourceSP.class);
		job1.setMapperClass(ConvertDateFormatMapper.class);
		job1.setReducerClass(ConvertDateFormatReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(rawInput));
		FileOutputFormat.setOutputPath(job1, new Path(input));
		// Just in case force to be only one reducer for data format transform in AWS environment
//		job1.setNumReduceTasks(1);
		
		job1.waitForCompletion(true);
	}
	
	
	public static void writeFinalResult(String queryNodeId, String readFolder, String input, String output ) throws Exception {
		Configuration conf3 = new Configuration();
		// Passing the queryNodeId to be used in the reducer
        conf3.set("queryNodeId", queryNodeId);
        
        Job job3 = Job.getInstance( conf3, "write the final result" );
        job3.setJarByClass(SingleSourceSP.class);
		job3.setMapperClass(FinalWriteMapper.class);
		job3.setReducerClass(FinalWriteReducer.class);
		
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(LongWritable.class);

		// Use the latest output path as the input path to extract and generate the final result 
//		FileInputFormat.addInputPath(job3, new Path(readFile+"/part-r-00000"));
		FileInputFormat.addInputPath(job3, new Path(readFolder));
		
		// Set the output path as the concatenation of prefix name of the input file name and queryNodeId. 
		// e.g. input: NA.cedge.txt/queryNodeId: 0, then the output folder path would be: output/NA0
		String[] fileInputPath = input.split("/");
		String filePrefixName = fileInputPath[fileInputPath.length-1].split("\\.")[0];
		String finalOutputPath = output + "/" + filePrefixName + queryNodeId;
		FileOutputFormat.setOutputPath(job3, new Path(finalOutputPath));
		// Just in case force to be only one reducer for final result output in AWS environment 
//		job3.setNumReduceTasks(1);
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
	
}
