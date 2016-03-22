package hello.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.yarn.client.YarnClient;

@EnableAutoConfiguration
@SpringBootApplication
public class ClientApplication {

	public static void main(String[] args) {
         SpringApplication.run(ClientApplication.class, args);
	}
}
