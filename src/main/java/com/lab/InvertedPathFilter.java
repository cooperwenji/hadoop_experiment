package com.lab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.nutch.parse.ParseText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvertedPathFilter implements PathFilter{
	    private static final Logger logger = LoggerFactory.getLogger(InvertedPathFilter.class);
	
		public boolean accept(Path path) {
			String name = path.toString();
			
			//logger.info("path name: " + name);
			return name.contains("parse_text/part-00000/data")||name.contains("parse_text/part-00001/data");
		}
}


