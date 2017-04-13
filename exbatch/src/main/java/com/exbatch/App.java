package com.exbatch;

import com.exbatch.config.AppConfig;
import com.exbatch.config.BatchConfig;
import com.exbatch.config.DBConfig;
import com.exbatch.dao.MyDAO;
import com.exbatch.domain.User;
import com.exbatch.service.MyService;
import org.apache.log4j.Logger;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class App {
    @Autowired
    @Qualifier("sampleJob")
    Job sampleJob;

    @Autowired
    @Qualifier("sampleDbJob")
    Job sampleDbJob;

    @Autowired
    SimpleJobLauncher simpleJobLauncher;

    @Autowired
    MyDAO dao;

    @Autowired
    MyService myService;

    static Logger logger = Logger.getLogger(App.class);

    public void runBatch() throws Exception {

        /*
        JobExecution jobExecution = simpleJobLauncher.run(sampleJob, new JobParameters());
        BatchStatus batchStatus = jobExecution.getStatus();
        logger.info(batchStatus);
        */


        JobExecution jobExecution = simpleJobLauncher.run(sampleDbJob, new JobParameters());
        BatchStatus batchStatus = jobExecution.getStatus();
        logger.info(batchStatus);

    }

    public void runDAO() throws Exception {
        logger.info("SA : " + dao.getSuperAdmin("SA"));

        List<String> nameList = dao.getAdminUserNameList();
        logger.info(nameList);

        nameList = dao.getAllUserNameList();
        logger.info(nameList);

        dao.updateAdminName();
        nameList = dao.getAllUserNameList();
        logger.info(nameList);

        List<String> userEmailList = dao.findAdminEMail();
        logger.info(userEmailList);

        List<User> allUsers = dao.getAllUser();
        List<User> pagingUsers = dao.getPagingUserList(2, 3);

        logger.info(allUsers);
        logger.info(pagingUsers);

        dao.setBatchOffset("mybatch", 777);
        System.out.println("batch offset : " + dao.getBatchOffset("mybatch"));
    }

    public void runTest() throws Exception {

        //dao.insertBatch2();
        //dao.updateBatch2();
        //dao.insertBatch2();
    }

    public void runBulkInsert() {
        List<User> users = new ArrayList<>();

        users.add(new User("ebs", "ebs", "ebs@google.co.kr", "GUEST"));
        users.add(new User("kbs", "kbs", "kbs@google.co.kr", "GUEST"));
        users.add(new User("sbs", "sbs", "sbs@google.co.kr", "GUEST"));
        users.add(new User("jtbc", "jtbc 손석희", "jtbc@google.co.kr", "GUEST"));

        dao.bulkInsert(users);
    }

    public void runBulkUpdate() {
        List<String> list = new ArrayList<>();

        list.add("mbc");
        list.add("sbs");
        list.add("kbs");

        dao.bulkUpdate(list);
    }

    public void runDBService() throws Exception {
        myService.saveAdmins();
    }

    public static void main( String[] args ) {

        AnnotationConfigApplicationContext context = null;

        try {
            context = new AnnotationConfigApplicationContext(AppConfig.class);
           // org.apache.ibatis.logging.LogFactory.useLog4JLogging();
            App app = context.getBean(App.class);
            //app.runDAO();
            //app.runDBService();
            app.runBatch();
            //app.runTest();

        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            if (null != context) {
                context.close();
            }
        }
    }
}
