package org.javacream.training.hadoop.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsReadFile {


	
	private void readFile() throws Exception {
		String uri = "hdfs://localhost:9000/user/root/demo.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		in = fs.open(new Path(uri));
		IOUtils.copyBytes(in, System.out, 4096, false);
		IOUtils.closeStream(in);
	}
	
	public static void main(String[] args) throws Exception {
		new HdfsReadFile().readFile();
	}
}
