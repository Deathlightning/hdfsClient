package com.ulric.hdfs_client.bean;

import lombok.Data;

@Data
public class FileInfo {
    private String fileName;
    private long size;
    private String updateTime;
    private boolean directory;
}
