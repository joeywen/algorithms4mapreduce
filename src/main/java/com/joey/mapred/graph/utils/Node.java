package com.joey.mapred.graph.utils;

import org.apache.hadoop.io.Text;

import com.joey.mapred.utils.ArrayListWritable;

public class Node {
	/**
	 * three possible colors a node can have to keep track of the visiting
	 * status of the nodes during graph search
	 */
	public static enum Color {
		WHITE((byte) 0), // unvisited
		GRAY((byte) 1), // visited, unprocess
		BLACK((byte) 2); // processed
		
		public byte val;

	    private Color(byte v) {
	      this.val = v;
	    }
	};

	private String id; // id of the node
	private int distance; // distance of the node from source node
	// list of the edges
	private ArrayListWritable<Text> edges = new ArrayListWritable<Text>();
	private Color color = Color.WHITE;

	// parent/ predecessor of the node
	// The parent of the source is marked "source" to leave it unchanged
	private String parent;

	public Node() {
		distance = Integer.MAX_VALUE;
		color = Color.WHITE;
		parent = null;
	}

	public Node(String nodeInfo) {
		String[] inputVal = nodeInfo.split("\t");
		String key = "";
		String val = "";

		try {
			key = inputVal[0]; // node id
			// the list of adjacent nodes, distance, color, parent
			val = inputVal[1];
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		String[] tokens = val.split("\\|");
		this.id = key;
		for (String s : tokens[0].split(",")) {
			if (s.length() > 0)
				edges.add(new Text(s));
		}

		if (tokens[1].equalsIgnoreCase("Integer.MAX_VALUE")) {
			this.distance = Integer.MAX_VALUE;
		} else {
			this.distance = Integer.parseInt(tokens[1]);
		}

		this.color = Color.valueOf(tokens[2]);
		this.parent = tokens[3];
	}

	public Text getNodeInfo() {
		StringBuilder sb = new StringBuilder();
		for (Text v : edges) {
			sb.append(v.toString()).append(",");
		}

		sb.append("|");

		if (this.distance < Integer.MAX_VALUE) {
			sb.append(this.distance).append("|");
		} else {
			sb.append("Integer.MAX_VALUE").append("|");
		}

		sb.append(color.toString()).append("|");
		sb.append(getParent());

		return new Text(sb.toString());
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public ArrayListWritable<Text> getEdges() {
		return edges;
	}

	public void setEdges(ArrayListWritable<Text> edges) {
		this.edges = edges;
	}

	public Color getColor() {
		return color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

}
