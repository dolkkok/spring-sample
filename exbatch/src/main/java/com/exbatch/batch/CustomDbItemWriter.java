package com.exbatch.batch;

import com.exbatch.dao.MyDAO;
import com.exbatch.domain.User;
import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.Properties;

public class CustomDbItemWriter implements ItemWriter<List<User>> {

    @Autowired
    MyDAO dao;

    @Autowired
    @Qualifier("batchProperties")
    Properties batchProperties;

    Logger logger = Logger.getLogger(CustomDbItemWriter.class);

    @Override
    public void write(List<? extends List<User>> userList) throws Exception {
        List<User> users = userList.get(0);

        try {
            dao.bulkInsert(users);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String offset = batchProperties.getProperty("batch_offset");
        String batchName = batchProperties.getProperty("batch_name");
        dao.setBatchOffset(batchName, Long.parseLong(offset));
        logger.info("insert users : " + users);
    }
}
