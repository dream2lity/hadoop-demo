package com.sunlands.mapjoin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chijiuwang@sunlands.com
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Map<String, String> localCache = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI cacheFile = context.getCacheFiles()[0];
        try (FileSystem fileSystem = FileSystem.newInstance(cacheFile, context.getConfiguration())) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(cacheFile))))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] split = line.split(",");
                    localCache.put(split[0], line);
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String orgId = split[3];
        context.write(new Text(orgId), new Text(localCache.getOrDefault(orgId, "") + "\t" + value.toString()));
    }
}
