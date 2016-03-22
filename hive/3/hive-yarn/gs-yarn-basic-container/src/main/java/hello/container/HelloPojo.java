package hello.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;

@YarnComponent
public class HelloPojo {

	private static final Log log = LogFactory.getLog(HelloPojo.class);

	@Autowired
	private Configuration configuration;

	@OnContainerStart
	public void publicVoidNoArgsMethod() throws Exception {
		log.info("Request result:");
		FileSystem fs = FileSystem.get(URI.create("hdfs://10.0.2.15"), new Configuration());
        String uri = "/user/root/planes.output/stdout";
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
        String line = null;
        while ((line = reader.readLine()) != null) {
            log.info(line);
        }
        log.info("Done.");
	}

}
