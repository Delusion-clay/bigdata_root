package com.cn.wifi.flume.source;

import com.cn.wifi.flume.constant.ConstantFields;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.io.File.separator;

public class FoldSource extends AbstractSource implements Configurable, PollableSource {
    private final Logger LOG = Logger.getLogger(FoldSource.class);
    //配置参数
    //文件监控目录
    private String fileDir;
    //文件监控目录
    private String succDir;
    //每批处理的最大文件数
    private int fileNum;
    //存放每批次文件的集合
    private List<File> listFiles;

    private List<Event> listEvents;

    @Override
    public void configure(Context context) {
        //配置文件读取
        listEvents = new ArrayList<>();
        fileDir = context.getString("fileDir");
        succDir = context.getString("succDir");
        fileNum = context.getInteger("fileNum");
        LOG.error("初始化参数fileDir=>" + fileDir);
        LOG.error("初始化参数succDir=>" + succDir);
        LOG.error("初始化参数fileNum=>" + fileNum);

    }

    @Override
    public Status process() throws EventDeliveryException {
        //TODO 程序存在的问题
        // 1.目录文件数过多
        // 2.flume批量发送
        Status status = null;
        //TODO 实时监控文件目录，文件解析
        //TODO FTP文件备份(主要用于数据恢复)
        //TODO FTP异常文件处理
        //获取目录下的所有文件  10000个
        LOG.info("=================开始读取文件=================");

        List<File> files = (List<File>) FileUtils.listFiles(new File(fileDir), new String[]{"txt"}, true);
        int fileCount = files.size();
        if (fileCount > fileNum) {
            listFiles = files.subList(0, fileNum);
        } else {
            listFiles = files;
        }
        if (listFiles.size() > 0) {
            for (File file : listFiles) {
                //获取文件名
                String fileName = file.getName();
                //FTP文件备份(主要用于数据恢复)
                //备份数据按天备份，按天分目录
                String succDirNew = succDir + separator + "2021-03-19";
                //System.out.println("文件备份目录" + succDirNew);
                //最终备份的绝对路径
                String fileNameNew = succDirNew + separator + fileName;
                LOG.info("最终备份的绝对路径"+"===========>"+fileNameNew);
                try {
                    if (new File(fileNameNew).exists()) {
                        //如果存在说明已经处理过了
                    } else {
                        List<String> lines = FileUtils.readLines(file);
                        //封装event 批量发送
                        lines.forEach(line -> {
                            System.out.println(line);
                            Event e = new SimpleEvent();
                            e.setBody(line.getBytes());
                            HashMap<String, String> header = new HashMap<>();
                            header.put(ConstantFields.FILE_NAME,fileName);
                            header.put(ConstantFields.ABSOLUTE_FILENAME,fileNameNew);
                            e.setHeaders(header);
                            listEvents.add(e);
                        });
                        LOG.info("==================开始文件备份==============");
                        FileUtils.moveToDirectory(file, new File(succDirNew), true);
                    }
                    getChannelProcessor().processEventBatch(listEvents);
                    listEvents.clear();
                    status = Status.READY;
                } catch (IOException e) {
                    status = Status.BACKOFF;
                    e.printStackTrace();
                }
            }
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
