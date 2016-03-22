package hello.client.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;

@Controller
public class ApplicationStartController {

    @Autowired
    private ApplicationContext context;

    @RequestMapping(value = "/submit-webhcat-query", method= RequestMethod.GET)
    public @ResponseBody String submitWebhcatQuery(@RequestParam("manufacturer") String manufacturer) {
        String url = "http://localhost:50111/templeton/v1/hive?user.name=root";
        MultiValueMap<String, String> parametersMap = new LinkedMultiValueMap<String, String>();
        parametersMap.add("execute", "select * from hbase_planes where type = '" + manufacturer + "';");
        parametersMap.add("callback", "http://localhost:9000/process-callback");
        parametersMap.add("statusdir", "planes.output");
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.postForObject(url, parametersMap, String.class);
        return "Query is submitted!\n";
    }

    @RequestMapping(value = "/process-callback", method= RequestMethod.GET)
    public @ResponseBody String processCallback() {
        context.getBean(YarnClient.class).submitApplication();
        return "Process is handled!\n";
    }
}
