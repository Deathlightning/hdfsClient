package com.ulric.hdfs_client.bean;

import lombok.Data;

@Data
public class Result {
    private int code = 200;
    private String message;
    private Object data;

    public Result(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public Result() {
    }

    public Result(Object data) {
        this.data = data;
    }
}
