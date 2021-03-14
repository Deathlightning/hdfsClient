package com.ulric.hdfs_client;

import com.ulric.hdfs_client.bean.FileInfo;
import com.ulric.hdfs_client.bean.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@RequestMapping("/")
@RestController
public class MainController {
    @Value("${hadoop.home}")
    private String hadoopHome;
    @Value("${hadoop.url}")
    private String url;
    @Value("${hadoop.user}")
    private String user;

    private Logger logger = LogManager.getLogger(this.getClass());
    private FileSystem fileSystem = null;

    @PostConstruct
    public void init() {
        System.setProperty("hadoop.home.dir", hadoopHome);
        Configuration configuration = new Configuration();
        try {
            fileSystem = FileSystem.get(new URI(url), configuration, user);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/ls")
    public Result ls(String path) {
        try {
            FileStatus[] statuses = fileSystem.listStatus(new Path(path));
            List<FileInfo> fileInfoList = new ArrayList<>();
            DateTimeFormatter ftf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            for (FileStatus file : statuses) {
                FileInfo fileInfo = new FileInfo();
                fileInfo.setFileName(file.getPath().getName());
                fileInfo.setDirectory(file.isDirectory());
                fileInfo.setSize(file.getLen());
                fileInfo.setUpdateTime(ftf.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(file.getModificationTime()), ZoneId.systemDefault())));
                ;
                fileInfoList.add(fileInfo);
            }
            return new Result(fileInfoList);
        } catch (IllegalArgumentException | IOException e) {
            logger.error("遍历文件出错", e);
        }
        return new Result(-1, "path不存在");
    }

    @PostMapping("/put")
    public Result put(MultipartFile file, String path) {
        if (file.isEmpty()) {
            return new Result(-1, "上传文件异常");
        }
        try {
            boolean fileExist = fileSystem.exists(new Path(path));
            if (fileExist) {
                return new Result(-1, "文件已存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        InputStream input = null;
        OutputStream output = null;
        try {
            input = file.getInputStream();
            output = fileSystem.create(new Path(path));
            IOUtils.copyBytes(input, output, 4096, true);
        } catch (IllegalArgumentException | IOException e) {
            logger.error("上传文件出错", e);
            return new Result(-1, "上传失败");
        }
        return new Result();
    }

    @GetMapping("/download")
    public Result download(HttpServletResponse response, String path) {
        InputStream input = null;
        try {
            input = fileSystem.open(new Path(path));
            IOUtils.copyBytes(input, response.getOutputStream(), 4096, true);
            return new Result();
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
            return new Result(-1, "下载失败");
        }
    }

    @GetMapping("/delete")
    public Result delete(String path) {
        try {
            boolean result = fileSystem.delete(new Path(path), true);
            System.out.println(path + " " + result);
            if (result) {
                return new Result();
            }
            return new Result(-1, "删除失败");
        } catch (IllegalArgumentException | IOException e) {
            logger.error("删除文件出错", e);
        }
        return new Result();
    }

}
