/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.divingwai.mapreduce.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CustKey implements Writable {
    protected Text customerId;
    //default constructor
    public CustKey()
    {
        super();
        customerId=new Text();
    }
    public CustKey(Text customerId)
    {
        super();
        this.customerId=customerId;
    }
    public CustKey(String customerId)
    {
        super();
        this.customerId=new Text(customerId);
    }
    public CustKey(CustKey k)
    {
        super();
        this.customerId=k.customerId;
    }
    /**
     * @return the customerId
     */
    public Text getCustomerId() {
        return customerId;
    }
    /**
     * @param customerId the customerId to set
     */
    public void setCustomerId(Text customerId) {
        this.customerId = customerId;
    }
    public void setCustomerId(String customerId) {
        this.customerId = new Text(customerId);
    }

    public void readFields(DataInput arg0) throws IOException {
        this.customerId.readFields(arg0);
    }


    public void write(DataOutput arg0) throws IOException {
        this.customerId.write(arg0);
    }



    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((customerId == null) ? 0 : customerId.hashCode());
        return result;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CustKey other = (CustKey) obj;
        if (customerId == null) {
            if (other.customerId != null)
                return false;
        } else if (!customerId.equals(other.customerId))
            return false;
        return true;
    }
}
