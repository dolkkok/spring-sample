package com.exbatch.batch;

import java.util.List;

import com.exbatch.domain.User;
import com.exbatch.repository.StringDB;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.StringMatchFilter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;

public class CustomItemReader implements ItemReader<String> {

    @Autowired
    StringDB alphabetRepository;
    private int count = 0;

    Logger logger = Logger.getLogger(CustomItemReader.class);

    @Override
    public String read() throws Exception, UnexpectedInputException, ParseException {


        if (count < alphabetRepository.size()) {
            String item = alphabetRepository.get(count++);
            logger.info("read item : " + item);

            return item;

        } else {
            logger.info("read item : null");
            return null;
        }
    }
}
