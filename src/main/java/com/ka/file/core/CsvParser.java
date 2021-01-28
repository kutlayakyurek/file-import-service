package com.ka.file.core;

import au.com.bytecode.opencsv.CSVReader;
import com.amazonaws.services.dynamodbv2.document.Item;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

public class CsvParser {

  public static List<Item> parse(BufferedReader reader) throws Exception {
    CSVReader csvReader = new CSVReader(reader);
    List<String[]> list = csvReader.readAll();
    List<Item> itemList = new ArrayList<Item>();
    Item newItem;

    newItem = new Item();
    Long dateTime = System.currentTimeMillis() / 1000;

    newItem.withString("Content", list.toString());
    newItem.withPrimaryKey("Id", String.valueOf(System.currentTimeMillis()), "Date", dateTime);

    itemList.add(newItem);

    reader.close();
    csvReader.close();

    return itemList;
  }

}
