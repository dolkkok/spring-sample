package com.tutorial.domain;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.util.xml.StaxUtils;

import java.util.ArrayList;
import java.util.List;

/*
import java.util.ArrayList;app.insertDocument("1", "Tomas", "10", "Hello Tomas 2017",
                    Arrays.asList("mbc", "sbs", "ebs", "kbs"),
                    Arrays.asList(new Comment("Jonh Smith", "2017"), new Comment("Alice White", "2018")));

                    .field("name", name)
                    .field("age", age)
                    .field("memo", memo)
                    .field("studio", studio)
                    .startArray("comments");
                        ld("name", c.getName())
                        .field("date", c.getDate())
*/
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NON_PRIVATE)
public class Student {
    @JsonIgnore
    String docId;

    @JsonProperty("name")
    String name;

    @JsonProperty("age")
    String age;

    @JsonProperty("memo")
    String memo;

    @JsonProperty("studio")
    List<String> studio;

    @JsonProperty("comments")
    List<Comment> comments;

    public Student() {
        studio = new ArrayList<String>();
        comments = new ArrayList<Comment>();
    }

    public Student(String docId, String name, String age, String memo, List<String> studio) {
        this.docId = docId;
        this.name = name;
        this.age = age;
        this.memo = memo;
        this.studio = studio;
        this.comments = new ArrayList<Comment>();
    }

    public Student(String docId, String name, String age, String memo, List<String> studio, List<Comment> comments) {
        this.docId = docId;
        this.name = name;
        this.age = age;
        this.memo = memo;
        this.studio = studio;
        this.comments = comments;
    }

    public String getDocId() {
        return this.docId;
    }

    public String getName() {
        return this.name;
    }
}
