package com.sunlands;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author chijiuwang@sunlands.com
 */
public class HdfsTest {

    @Test
    public void fetchFromURI() throws IOException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        InputStream inputStream = new URL("hdfs://172.16.117.73:9000/user/eagle/test/a.txt").openStream();

        byte[] buffer = new byte[1024];
        int len;
        StringBuilder stringBuilder = new StringBuilder();
        FileOutputStream outputStream = new FileOutputStream("fromHDFS.txt");
        while ((len = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, len);
            for (int i = 0; i < len; i++) {
                stringBuilder.append((char) buffer[i]);
            }
            System.out.println(stringBuilder.toString());
        }
        outputStream.flush();
        /*IOUtils.copy(inputStream, outputStream);*/

        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    @Test
    public void getFileSystem() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://172.16.117.73:9000/");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem);

        FileSystem fileSystem1 = FileSystem.newInstance(configuration);
        System.out.println(fileSystem1);

        FileSystem fileSystem2 = FileSystem.get(new URI("hdfs://172.16.117.73:9000"), configuration, "eagle");
        System.out.println(fileSystem2);

        FileSystem fileSystem3 = FileSystem.newInstance(new URI("hdfs://172.16.117.73:9000"), configuration);
        System.out.println(fileSystem3);
    }

    @Test
    public void getFileList() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.117.73:9000"), new Configuration(), "eagle");
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/user/eagle/test"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath() + " -> " + fileStatus.getPath().getName());
        }
        fileSystem.close();
    }

    @Test
    public void upload() throws URISyntaxException, IOException, InterruptedException {
        try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://172.16.117.73:9000"), new Configuration(), "eagle")) {
            fileSystem.copyFromLocalFile(new Path("input/word_count.txt"), new Path("/user/eagle/test/"));
        }
    }

}
