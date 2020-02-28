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
import org.apache.hadoop.io.WritableComparable;

public class CustValue implements WritableComparable<CustValue> {
    protected CustKey custId;
    protected Text firstName,lastName,age,profession;
    public CustValue()
    {
        super();
        custId=new CustKey();
        firstName=new Text();
        lastName=new Text();
        age=new Text();
        profession=new Text();
    }

    public CustValue(CustKey custId, Text firstName, Text lastName, Text age,
            Text profession) {
        super();
        this.custId = new CustKey(custId);
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.profession = profession;
    }
    public CustValue(String custId, String firstName, String lastName, String age,
            String profession) {
        super();
        this.custId = new CustKey(custId);
        this.firstName = new Text(firstName);
        this.lastName =new Text(lastName);
        this.age = new Text(age);
        this.profession = new Text(profession);
    }
    /**
     * @return the custId
     */
    public CustKey getCustId() {
        return custId;
    }

    /**
     * @param custId the custId to set
     */
    public void setCustId(CustKey custId) {
        this.custId = custId;
    }

    /**
     * @return the firstName
     */
    public Text getFirstName() {
        return firstName;
    }

    /**
     * @param firstName the firstName to set
     */
    public void setFirstName(Text firstName) {
        this.firstName = firstName;
    }

    /**
     * @return the lastName
     */
    public Text getLastName() {
        return lastName;
    }

    /**
     * @param lastName the lastName to set
     */
    public void setLastName(Text lastName) {
        this.lastName = lastName;
    }

    /**
     * @return the age
     */
    public Text getAge() {
        return age;
    }

    /**
     * @param age the age to set
     */
    public void setAge(Text age) {
        this.age = age;
    }

    /**
     * @return the profession
     */
    public Text getProfession() {
        return profession;
    }

    /**
     * @param profession the profession to set
     */
    public void setProfession(Text profession) {
        this.profession = profession;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        this.custId.readFields(arg0);
        this.age.readFields(arg0);
        this.profession.readFields(arg0);
        this.lastName.readFields(arg0);
        this.firstName.readFields(arg0);
        }

    @Override
    public void write(DataOutput arg0) throws IOException {
        this.custId.write(arg0);
        this.age.write(arg0);
        this.profession.write(arg0);
        this.lastName.write(arg0);
        this.firstName.write(arg0);
        }

    @Override
    public int compareTo(CustValue o) {
        /*
         * Here we're gonna compare customerid and the age
         */
        int comp=this.custId.customerId.compareTo(o.custId.customerId);
        if(comp!=0)
        {
            return comp;
        }
        else return this.age.compareTo(o.age);
    }

}
