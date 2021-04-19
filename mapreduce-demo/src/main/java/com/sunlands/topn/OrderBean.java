package com.sunlands.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author chijiuwang@sunlands.com
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String id;
    private Double price;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id='" + id + '\'' +
                ", price=" + price +
                '}';
    }

    @Override
    public int compareTo(OrderBean o) {
        int i = this.id.compareTo(o.id);
        if (i == 0) {
            i = this.price.compareTo(o.price) * -1;
        }
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.price = in.readDouble();
    }
}
