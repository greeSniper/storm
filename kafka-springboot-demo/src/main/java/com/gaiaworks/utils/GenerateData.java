package com.gaiaworks.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by 唐哲
 * 2018-02-19 16:32
 */
public class GenerateData {

    public static String generateName() {
        String[] firstNameList = { "赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈",
                "楮", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许", "何", "吕",
                "施", "张", "孔", "曹", "严", "华", "金", "魏", "陶", "姜", "戚", "谢",
                "邹", "喻", "柏", "水", "窦", "章", "云", "苏", "潘", "葛", "奚", "范",
                "彭", "郎", "鲁", "韦", "昌", "马", "苗", "凤", "花", "方", "俞", "任",
                "袁", "柳", "酆", "鲍", "史", "唐", "费", "廉", "岑", "薛", "雷", "贺",
                "倪", "汤", "滕", "殷", "罗", "毕", "郝", "邬", "安", "常", "乐", "于",
                "时", "傅", "皮", "卞", "齐", "康", "伍", "余", "元", "卜", "顾", "孟",
                "平", "黄", "和", "穆", "萧", "尹", "姚", "邵", "湛", "汪", "祁", "毛",
                "禹", "狄", "米", "贝", "明", "臧", "计", "伏", "成", "戴", "宋", "公",
                "茅", "庞", "熊", "纪", "舒", "屈", "项", "祝", "董", "梁", "杜", "阮",
                "蓝", "闽", "席", "季", "麻", "强", "贾", "路", "娄", "危", "江", "童",
                "颜", "郭", "梅", "盛", "林", "刁", "锺", "徐", "丘", "骆", "高", "夏",
                "蔡", "田", "樊", "胡", "凌", "霍", "虞", "万", "支", "柯", "昝", "管",
                "卢", "莫", "经", "房", "裘", "缪", "干", "解", "应", "宗", "丁", "宣",
                "贲", "邓", "郁", "单", "杭", "洪", "包", "诸", "左", "石", "崔", "吉",
                "钮", "龚", "程", "嵇", "邢", "滑", "裴", "陆", "荣", "翁", "荀", "羊",
                "於", "惠", "甄", "麹", "家", "封", "芮", "羿", "储", "靳", "汲", "邴",
                "糜", "松", "井", "段", "富", "巫", "乌", "焦", "巴", "弓", "牧", "隗",
                "山", "谷", "车", "侯", "宓", "蓬", "全", "郗", "班", "仰", "秋", "仲",
                "伊", "宫", "宁", "仇", "栾", "暴", "甘", "斜", "厉", "戎", "祖", "武",
                "符", "刘", "景", "詹", "束", "龙", "叶", "幸", "司", "韶", "郜", "黎",
                "蓟", "薄", "印", "宿", "白", "怀", "蒲", "邰", "从", "鄂", "索", "咸",
                "籍", "赖", "卓", "蔺", "屠", "蒙", "池", "乔", "阴", "郁", "胥", "能",
                "苍", "双", "闻", "莘", "党", "翟", "谭", "贡", "劳", "逄", "姬", "申",
                "扶", "堵", "冉", "宰", "郦", "雍", "郤", "璩", "桑", "桂", "濮", "牛",
                "寿", "通", "边", "扈", "燕", "冀", "郏", "浦", "尚", "农", "温", "别",
                "庄", "晏", "柴", "瞿", "阎", "充", "慕", "连", "茹", "习", "宦", "艾",
                "鱼", "容", "向", "古", "易", "慎", "戈", "廖", "庾", "终", "暨", "居",
                "衡", "步", "都", "耿", "满", "弘", "匡", "国", "文", "寇", "广", "禄",
                "阙", "东", "欧", "殳", "沃", "利", "蔚", "越", "夔", "隆", "师", "巩",
                "厍", "聂", "晁", "勾", "敖", "融", "冷", "訾", "辛", "阚", "那", "简",
                "饶", "空", "曾", "毋", "沙", "乜", "养", "鞠", "须", "丰", "巢", "关",
                "蒯", "相", "查", "后", "荆", "红", "游", "权", "逑", "盖", "益", "桓" };
        String[] secondNameList = { "伟", "伟", "芳", "伟", "秀英", "秀英", "娜", "秀英", "伟",
                "敏", "静", "丽", "静", "丽", "强", "静", "敏", "敏", "磊", "军", "洋",
                "勇", "勇", "艳", "杰", "磊", "强", "军", "杰", "娟", "艳", "涛", "涛",
                "明", "艳", "超", "勇", "娟", "杰", "秀兰", "霞", "敏", "军", "丽", "强",
                "平", "刚", "杰", "桂英", "芳", "　嘉懿", "煜城", "懿轩", "烨伟", "苑博", "伟泽",
                "熠彤", "鸿煊", "博涛", "烨霖", "烨华", "煜祺", "智宸", "正豪", "昊然", "明杰",
                "立诚", "立轩", "立辉", "峻熙", "弘文", "熠彤", "鸿煊", "烨霖", "哲瀚", "鑫鹏",
                "致远", "俊驰", "雨泽", "烨磊", "晟睿", "天佑", "文昊", "修洁", "黎昕", "远航",
                "旭尧", "鸿涛", "伟祺", "荣轩", "越泽", "浩宇", "瑾瑜", "皓轩", "擎苍", "擎宇",
                "志泽", "睿渊", "楷瑞", "子轩", "弘文", "哲瀚", "雨泽", "鑫磊", "修杰", "伟诚",
                "建辉", "晋鹏", "天磊", "绍辉", "泽洋", "明轩", "健柏", "鹏煊", "昊强", "伟宸",
                "博超", "君浩", "子骞", "明辉", "鹏涛", "炎彬", "鹤轩", "越彬", "风华", "靖琪",
                "明诚", "高格", "光华", "国源", "冠宇", "晗昱", "涵润", "翰飞", "翰海", "昊乾",
                "浩博", "和安", "弘博", "宏恺", "鸿朗", "华奥", "华灿", "嘉慕", "坚秉", "建明",
                "金鑫", "锦程", "瑾瑜", "晋鹏", "经赋", "景同", "靖琪", "君昊", "俊明", "季同",
                "开济", "凯安", "康成", "乐语", "力勤", "良哲", "理群", "茂彦", "敏博", "明达",
                "朋义", "彭泽", "鹏举", "濮存", "溥心", "璞瑜", "浦泽", "奇邃", "祺祥", "荣轩",
                "锐达", "睿慈", "绍祺", "圣杰", "晟睿", "思源", "斯年", "泰宁", "天佑", "同巍",
                "奕伟", "祺温", "文虹", "向笛", "心远", "欣德", "新翰", "兴言", "星阑", "修为",
                "旭尧", "炫明", "学真", "雪风", "雅昶", "阳曦", "烨熠", "英韶", "永贞", "咏德",
                "宇寰", "雨泽", "玉韵", "越彬", "蕴和", "哲彦", "振海", "正志", "子晋", "自怡",
                "德赫", "君平", "鲁尼" };

        int a = (int) Math.abs(firstNameList.length * Math.random());
        int b = (int) Math.abs(secondNameList.length * Math.random());
        return firstNameList[a] + secondNameList[b];
    }

    public static String generateWorkStartTime() {
        Date date = RandomDateOneDay.randomDate("2018-02-14 07:55:00", "2018-02-14 08:55:00"); //08:30之后迟到
        return new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(date);
    }

    public static String generateWorkEndTime() {
        Date date = RandomDateOneDay.randomDate("2018-02-14 17:20:00", "2018-02-14 18:30:00"); //17:30之前早退
        return new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(date);
    }

    public static String generateNoonStartTime() {
        Date date = RandomDateOneDay.randomDate("2018-02-14 11:55:00", "2018-02-14 12:05:00"); //12:00之前早退
        return new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(date);
    }

    public static String generateNoonEndTime() {
        Date date = RandomDateOneDay.randomDate("2018-02-14 12:50:00", "2018-02-14 13:05:00"); //13:00之后迟到
        return new SimpleDateFormat("yyyy.MM.dd HH:mm:ss").format(date);
    }

    public static String generateOneWorkStartRecord(long personId, String name) {
        String punchTime = generateWorkStartTime();
        return personId + "," + name + "," + punchTime;
    }

    public static String generateOneWorkEndRecord(long personId, String name) {
        String punchTime = generateWorkEndTime();
        return personId + "," + name + "," + punchTime;
    }

    public static String generateOneNoonStartRecord(long personId, String name) {
        String punchTime = generateNoonStartTime();
        return personId + "," + name + "," + punchTime;
    }

    public static String generateOneNoonEndRecord(long personId, String name) {
        String punchTime = generateNoonEndTime();
        return personId + "," + name + "," + punchTime;
    }

    public static final String TIME_ONE = ",2018.02.14 08:30:00, , ,2018.02.14 17:30:00";
    public static final String TIME_TWO = ",2018.02.14 08:30:00,2018.02.14 12:00:00,2018.02.14 13:00:00,2018.02.14 17:30:00";

    public static void generateBatch() throws IOException {
        String filePath = "D:\\IdeaProjects\\Gaiaworks\\imooc-demo\\data";
        //String filePath = "/home/hadoop/data/logs/access.log";
        File f = new File(filePath);

        BufferedWriter output = new BufferedWriter(new FileWriter(f, true));

        int i = 1;
        for(; i<=40; i++) {
            String name = generateName();
            output.write(generateOneWorkStartRecord(i, name) + TIME_ONE + "\r\n");
            output.write(generateOneWorkEndRecord(i, name) + TIME_ONE + "\r\n");
        }
        for(; i<=50; i++) {
            String name = generateName();
            output.write(generateOneWorkStartRecord(i, name) + TIME_TWO + "\r\n");
            output.write(generateOneNoonStartRecord(i, name) + TIME_TWO + "\r\n");
            output.write(generateOneNoonEndRecord(i, name) + TIME_TWO + "\r\n");
            output.write(generateOneWorkEndRecord(i, name) + TIME_TWO + "\r\n");
        }

        output.close();
    }

    /**
     * 返回一个map
     * key->personId
     * value->count
     */
    public static Integer[] generateBatch(Integer personId, Integer count) throws IOException {
        //String filePath = "D:\\IdeaProjects\\Gaiaworks\\imooc-demo\\data";
        String filePath = "/home/hadoop/data/logs/access.log";
        File f = new File(filePath);

        BufferedWriter output = new BufferedWriter(new FileWriter(f, true));

        int i = 1;
        for(; i<=4000; i++) {
            String name = generateName();
            output.write(generateOneWorkStartRecord(++personId, name) + TIME_ONE + "\r\n");
            output.write(generateOneWorkEndRecord(personId, name) + TIME_ONE + "\r\n");
            count += 2;
        }
        for(; i<=4500; i++) {
            String name = generateName();
            output.write(generateOneWorkStartRecord(++personId, name) + TIME_TWO + "\r\n");
            output.write(generateOneNoonStartRecord(personId, name) + TIME_TWO + "\r\n");
            output.write(generateOneNoonEndRecord(personId, name) + TIME_TWO + "\r\n");
            output.write(generateOneWorkEndRecord(personId, name) + TIME_TWO + "\r\n");
            count += 4;
        }

        output.close();

        Integer[] result = new Integer[2];
        result[0] = personId;
        result[1] = count;
        return result;
    }

    /**
     * 生成一名员工的随机数据
     */
    public static String generateOneRandomData(Integer personId) {
        StringBuilder sb = new StringBuilder();
        String name = generateName();
        if(personId%1000 != 0) {
            sb.append(generateOneWorkStartRecord(personId, name))
                    .append(TIME_ONE)
                    .append("|")
                    .append(generateOneWorkEndRecord(personId, name))
                    .append(TIME_ONE);
        } else {
           sb.append(generateOneWorkStartRecord(personId, name))
                   .append(TIME_TWO)
                   .append("|")
                   .append(generateOneNoonStartRecord(personId, name))
                   .append(TIME_TWO)
                   .append("|")
                   .append(generateOneNoonEndRecord(personId, name))
                   .append(TIME_TWO)
                   .append("|")
                   .append(generateOneWorkEndRecord(personId, name))
                   .append(TIME_TWO);
        }

        return sb.toString();
    }

    private static Integer personId = 1;
    private static String name = null;

    /**
     * 生成一名员工的一条打卡记录
     */
    public static String generateOne(Integer index) {
        if(index%6 == 0) {
            //没有午休打卡的上班打卡
            name = generateName();
            return generateOneWorkStartRecord(personId, name) + TIME_ONE;
        } else if(index%6 == 1) {
            //没有午休打卡的下班打卡
            return generateOneWorkEndRecord(personId++, name) + TIME_ONE;
        } else if(index%6 == 2) {
            //有午休打卡的上班打卡
            name = generateName();
            return generateOneWorkStartRecord(personId, name) + TIME_TWO;
        } else if(index%6 == 3) {
            //有午休打卡的午休前打卡
            return generateOneNoonStartRecord(personId, name) + TIME_TWO;
        } else if(index%6 == 4) {
            //有午休打卡的午休后打卡
            return generateOneNoonEndRecord(personId, name) + TIME_TWO;
        } else if(index%6 == 5) {
            //有午休打卡的下班打卡
            return generateOneWorkEndRecord(personId++, name) + TIME_TWO;
        } else {
            //其他
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        for(int i=0; i<100; i++) {
            System.out.println(generateOne(i));
        }
    }

}
