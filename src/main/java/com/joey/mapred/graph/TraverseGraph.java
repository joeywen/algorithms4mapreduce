package com.joey.mapred.graph;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.joey.mapred.graph.utils.Node;
import com.joey.mapred.graph.utils.Node.Color;

public class TraverseGraph {

	/**
	 * 
	 * Description : Mapper class that implements the map part of Breadth-first
	 * search algorithm. The nodes colored WHITE or BLACK are emitted as such. For
	 * each node that is colored GRAY, a new node is emitted with the distance
	 * incremented by one and the color set to GRAY. The original GRAY colored
	 * node is set to BLACK color and it is also emitted.
	 * 
	 * Input format <key, value> : <line offset in the input file (automatically
	 * assigned),
	 * nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent>
	 * 
	 * Output format <key, value> : <nodeId, (updated)
	 * list_of_adjacent_nodes|distance_from_the_source|color|parent node>
	 * 
	 * Reference :
	 * http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search
	 * -using-iterative-map-reduce-algorithm
	 * 
	 */

	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type
	public static class TraverseMapper extends Mapper<Object, Text, Text, Text> {
		protected void map(Object key, Text value, Context context, Node inNode)
				throws IOException, InterruptedException {
			if (inNode.getColor() == Color.GRAY) {
				for (String neighbor : inNode.getEdges()) {
					Node adjacentNode = new Node();
					adjacentNode.setId(neighbor);
					adjacentNode.setDistance(inNode.getDistance() + 1);
					adjacentNode.setColor(Node.Color.GRAY);
					adjacentNode.setParent(inNode.getId());

					context.write(new Text(adjacentNode.getId()),
							adjacentNode.getNodeInfo());
				}
				// this node is done, color it black
				inNode.setColor(Node.Color.BLACK);
			}

			context.write(new Text(inNode.getId()), inNode.getNodeInfo());
		}
	}

	/**
	 * 
	 * Description : Reducer class that implements the reduce part of parallel
	 * Breadth-first search algorithm. Make a new node which combines all
	 * information for this single node id that is for each key. The new node
	 * should have the full list of edges, the minimum distance, the darkest
	 * Color, and the parent/predecessor node
	 * 
	 * Input format <key,value> : <nodeId,
	 * list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
	 * 
	 * Output format <key,value> : <nodeId,
	 * (updated)list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
	 * 
	 */
	public static class TraverseReducer extends Reducer<Text, Text, Text, Text> {
		protected Node reduce(Text key, Iterable<Text> values, Context context,
				Node outNode) throws IOException, InterruptedException {
			// set the node id as the key
			outNode.setId(key.toString());

			for (Text value : values) {
				Node inNode = new Node(key.toString() + "\t" + value.toString());

				if (inNode.getEdges().size() > 0) {
					outNode.setEdges(inNode.getEdges());
				}

				if (inNode.getDistance() < outNode.getDistance()) {
					outNode.setDistance(inNode.getDistance());
					outNode.setParent(inNode.getParent());
				}

				if (inNode.getColor().ordinal() > outNode.getColor().ordinal()) {
					outNode.setColor(inNode.getColor());
				}

			}

			context.write(key, new Text(outNode.getNodeInfo()));

			return outNode;
		}
	}
}
