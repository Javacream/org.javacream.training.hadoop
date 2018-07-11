package org.javacream.training.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsWriteFile {


	private void writeFile() throws Exception {
		String uri = "hdfs://localhost:9000";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf, "root");
		Path name = new Path("/user/root/demo.txt");
		FSDataOutputStream stm = fs.create(name);
		stm.writeBytes("Hello Hadoop!");
		stm.close();
	}

	public static void main(String[] args) throws Exception {
		new HdfsWriteFile().writeFile();
	}

}
