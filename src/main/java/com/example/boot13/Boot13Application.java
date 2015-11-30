package com.example.boot13;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.cluster.leader.Candidate;
import org.springframework.cloud.cluster.leader.DefaultCandidate;
import org.springframework.cloud.cluster.leader.event.LeaderEventPublisher;
import org.springframework.cloud.cluster.leader.event.LeaderEventPublisherConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@RestController
@Import(LeaderEventPublisherConfiguration.class)
public class Boot13Application {

    @Autowired
    private LeaderEventPublisher publisher;

    public static void main(String[] args) {
        SpringApplication.run(Boot13Application.class, args);
    }

    @RequestMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> home(HttpSession session) {
        String value = (String) session.getAttribute("key1");
        if (value == null) {
            value = "value1";
            session.setAttribute("key1", value);
        }
        Map<String, Object> result = new HashMap<>();
        result.put("id", session.getId());
        result.put("value", value);

        return result;
    }

//    @Bean
//    public ClientCache clientCache() {
//        ClientCacheFactory factory = new ClientCacheFactory();
//        factory.addPoolLocator("192.168.56.101", 10334);
//        return factory.create();
//    }

    @Bean
    public Cache cache(){
        return new CacheFactory().create();
    }

    @Bean
    public Candidate gemfireLeaderCandidate() {
        return new DefaultCandidate();
    }

    @Bean
    public LeaderInitiator hazelcastLeaderInitiator(Cache clientCache, Candidate gemfireLeaderCandidate) {
        LeaderInitiator initiator = new LeaderInitiator(clientCache, gemfireLeaderCandidate);
        initiator.setLeaderEventPublisher(publisher);
        return initiator;
    }

}
