package com.li.ability.assessment;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class TestMongoDBReplSet {

    public static void main(String[] args) {

        List<ServerAddress> addresses = new ArrayList<ServerAddress>();
        ServerAddress address1 = new ServerAddress("192.168.100.153", 27017);
        ServerAddress address2 = new ServerAddress("192.168.100.154", 27017);
        ServerAddress address3 = new ServerAddress("192.168.100.155", 27017);
        addresses.add(address1);
        addresses.add(address2);
        addresses.add(address3);
        MongoClient client = new MongoClient(addresses);
        DB db = client.getDB("huatu_ztk");
        DBCollection coll = db.getCollection("ztk_answer_card");
        // 插入
        System.out.println(coll.count());
    }
}
