package com.icloud.parser;

import org.apache.parquet.example.data.Group;

public interface Parser<TYPE> {

    TYPE parse(Group group);
}
