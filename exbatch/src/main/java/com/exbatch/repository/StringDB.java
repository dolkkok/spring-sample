package com.exbatch.repository;

import org.springframework.stereotype.Repository;
import java.util.ArrayList;

@Repository
public class StringDB extends ArrayList<String> {

    public StringDB() {
        add("aaa");
        add("bbb");
        add("ccc");
        add("ddd");
        add("eee");
    }
}


