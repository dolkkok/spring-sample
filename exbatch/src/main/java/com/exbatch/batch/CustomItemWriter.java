package com.exbatch.batch;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemWriter;

public class CustomItemWriter implements ItemWriter<String> {

    Logger logger = Logger.getLogger(CustomItemWriter.class);

    @Override
    public void write(List<? extends String> alphabet) throws Exception {
        logger.info("write : " + alphabet);
    }
}
