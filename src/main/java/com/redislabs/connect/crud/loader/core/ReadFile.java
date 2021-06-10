package com.redislabs.connect.crud.loader.core;

import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

/**
 *
 * @author Virag Tripathi
 *
 */

@Slf4j
public class ReadFile {
    protected String[] numLines;
    protected StringBuffer sb;

    /**
     * @param fileName <fileName>Name of the file.</fileName>
     * @return String[] rows
     */
    public String readFileAsString(String fileName) throws Exception {
            sb = new StringBuffer();
            // be sure to not have line starting with "–" or "/*" or any other non alphabetical character
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                sb.append(currentLine);
            }
            br.close();

            // here is our splitter ! We use “;” as a delimiter for each request
            // then we are sure to have well formed statements
            numLines = sb.toString().split(";");

        return sb.toString();
    }

    /**
     * @param fileName <fileName>Name of the file.</fileName>
     * @return ArrayList result
     */
    public ArrayList<String> readFileAsList(String fileName) throws Exception {
        ArrayList<String> result = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
            while (br.ready()) {
                result.add(br.readLine());
            }
        return result;
    }

    /**
     * @param fileName <fileName>Name of the JSON file.</fileName>
     * @return org.json.simple.JSONArray result
     */
    public JSONArray readFileAsJson(String fileName) throws Exception {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(fileName));

        return (JSONArray) obj;
    }
}
