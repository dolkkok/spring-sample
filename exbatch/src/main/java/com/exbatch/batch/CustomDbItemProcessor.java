package com.exbatch.batch;

import com.exbatch.domain.User;
import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class CustomDbItemProcessor implements ItemProcessor<List<User>, List<User>> {

    Logger logger = Logger.getLogger(CustomDbItemProcessor.class);

    @Override
    public List<User> process(List<User> users) throws Exception {
        logger.info("process : " + users);

        Collection<User> newUsers = users
                .stream()
                .map(user -> {
                    User newUser = new User();
                    newUser = user;
                    newUser.setUserId(newUser.getUserId().toUpperCase());
                    return newUser;
                })
                .collect(Collectors.toList());


        //return alphabet.toUpperCase();
        return new ArrayList<>(newUsers);
    }
}

