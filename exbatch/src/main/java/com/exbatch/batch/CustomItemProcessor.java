package com.exbatch.batch;

import com.exbatch.App;
import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemProcessor;

public class CustomItemProcessor implements ItemProcessor<String, String> {

    Logger logger = Logger.getLogger(App.class);

    @Override
    public String process(String alphabet) throws Exception {
        logger.info("process : " + alphabet);
        return alphabet.toUpperCase();
    }
}

