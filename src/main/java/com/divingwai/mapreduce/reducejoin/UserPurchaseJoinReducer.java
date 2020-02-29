package com.divingwai.mapreduce.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
public class UserPurchaseJoinReducer extends Reducer<Text, Text, Text,
        Text> {
    private Text tmp = new Text();
    private ArrayList<Text> userList = new ArrayList<Text>();
    private ArrayList<Text> purchaseList = new ArrayList<Text>();
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        userList.clear();
        purchaseList.clear();
        
        System.out.println("Reducer is called");
        
//        for(Text text: values)
//        {
//             System.out.println(key.toString() + "," + text.toString());
//        }
        
//        while (values.iterator().hasNext()) {
//
//            tmp = values.iterator().next();
//                        System.out.println(key.toString() + "," + tmp.toString());
//            
//        }
//        

//        for(Text text: values)
//        {
//              if (text.toString().substring(0,1).equals("X")) {
//                  System.out.println("adding user");
//                userList.add(new Text(text.toString().substring(1)));
//            } else if (text.toString().substring(0,1).equals("Y")) {
//                 System.out.println("adding purchase");
//                purchaseList.add(new Text(text.toString().substring(1)));
//            }
//        }
//        
                
        
        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();
            if (tmp.toString().substring(0,1).equals("X")) {
                userList.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.toString().substring(0,1).equals("Y")) {
                purchaseList.add(new Text(tmp.toString().substring(1)));
            }
        }

        /* Joining both dataset */
        
         System.out.printf("userlist is empyt: %b", userList.isEmpty());
         System.out.printf("purchaseList is empyt: %b", purchaseList.isEmpty());
        

        if (!userList.isEmpty() && !purchaseList.isEmpty()) {
            for (Text user : userList) {
                for (Text purchase : purchaseList) {
                    context.write(user, purchase);
                }
            }
        }
    }
}
