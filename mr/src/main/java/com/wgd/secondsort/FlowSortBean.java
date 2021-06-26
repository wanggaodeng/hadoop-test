package com.wgd.secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author wang
 * @version 1.0
 * @description: 二次排序
 * @date 2021/6/26 19:14
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {
    private String phone;
    //上行包个数
    private Integer upPackNum;
    //下行包个数
    private Integer downPackNum;
    //上行总流量
    private Integer upPayload;
    //下行总流量
    private Integer downPayload;

    /**
     * 先对下行包总个数升序排序；若相等，再按上行总流量进行降序排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowSortBean o) {
        int i = this.downPackNum.compareTo(o.downPackNum);
        if(i == 0){ //如果downPackNum相等
            int i1 = this.upPayload.compareTo(o.upPayload);
            return -i1;

        }
        return i;
    }

    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeInt(upPackNum);
        out.writeInt(downPackNum);
        out.writeInt(upPayload);
        out.writeInt(downPayload);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.upPackNum = in.readInt();
        this.downPackNum = in.readInt();
        this.upPayload = in.readInt();
        this.downPayload = in.readInt();
    }

    public FlowSortBean() {
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Integer getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(Integer upPackNum) {
        this.upPackNum = upPackNum;
    }

    public Integer getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(Integer downPackNum) {
        this.downPackNum = downPackNum;
    }

    public Integer getUpPayload() {
        return upPayload;
    }

    public void setUpPayload(Integer upPayload) {
        this.upPayload = upPayload;
    }

    public Integer getDownPayload() {
        return downPayload;
    }

    public void setDownPayload(Integer downPayload) {
        this.downPayload = downPayload;
    }

    @Override
    public String toString() {
        return "FlowSortBean{" +
                "phone='" + phone + '\'' +
                ", upPackNum=" + upPackNum +
                ", downPackNum=" + downPackNum +
                ", upPayload=" + upPayload +
                ", downPayload=" + downPayload +
                '}';
    }
}
