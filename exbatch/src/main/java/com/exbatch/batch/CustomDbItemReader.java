package com.exbatch.batch;

import com.exbatch.dao.MyDAO;
import com.exbatch.domain.User;
import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.Properties;

public class CustomDbItemReader implements ItemReader<List<User>> {

    @Autowired
    MyDAO dao;

    @Autowired
    @Qualifier("batchProperties")
    Properties batchProperties;

    private int count = 0;

    Logger logger = Logger.getLogger(CustomDbItemReader.class);

    @Override
    public List<User> read() throws Exception, UnexpectedInputException, ParseException {

        String batchName = batchProperties.getProperty("batch_name");
        long offset = dao.getBatchOffset(batchName);

        //List<User> users = dao.getPagingUserList(offset, 2);
        List<User> users = dao.getQuickPagingUserList(offset, 2);
        batchProperties.setProperty("batch_offset", new Long(offset + users.size()).toString());

        if (users.size() > 0) {
            logger.info("read item : " + users);
            return users;
        }

        logger.info("read item : null");
        return null;
    }
}
