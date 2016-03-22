package com.epam.bigdata.web;

        import org.springframework.web.bind.annotation.RequestMapping;
        import org.springframework.web.bind.annotation.RestController;
        import org.springframework.web.client.RestTemplate;

@RestController
public class HelloController {
    @RequestMapping("/hello")
    public String runJob() {
        final String uri = "http://localhost:8080/springrestexample/employees.xml";

        RestTemplate restTemplate = new RestTemplate();
        String result = restTemplate.getForObject(uri, String.class);

        return result;
    }

    @RequestMapping("/callback")
    public String callback() {
        return "Call back!";
    }
}
