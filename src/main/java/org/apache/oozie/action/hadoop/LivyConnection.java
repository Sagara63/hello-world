package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

class LivyConnection {

    static final String SESSION_URI = "/sessions";
    static final String STATEMENT_URI = "/statements";
    static String LIVY_SESSION_ID = "";

    Map<String, String> getSessionStatus(Configuration actionConf, String sessionId) throws IOException, ParseException {
        String url = getBaseURI(actionConf) + LivyConnection.SESSION_URI + "/" + sessionId;
        HttpGet request = new HttpGet(url);
        HttpResponse response = executeLivyApi(request);
        checkStatus(response, 200);
        return getRespMap(getResponseAsString(response));
    }

    Map<String, String> deleteLivySession(Configuration actionConf, String sessionId) throws IOException, ParseException {
        Map<String, String> respOut = new HashMap<String, String>();
        if(sessionId != null && sessionId.length() > 0) {
            String url = getBaseURI(actionConf) + LivyConnection.SESSION_URI + "/" + sessionId;
            HttpDelete request = new HttpDelete(url);
            addReqHeaders(request);
            HttpResponse response = executeLivyApi(request);
            checkStatus(response, 200);
            respOut = getRespMap(getResponseAsString(response));
        }
        return respOut;
    }

    Map<String, String> getStatementStatus(Configuration actionConf, String sessionId, String stmtId) throws IOException, ParseException {
        String url = getBaseURI(actionConf) + LivyConnection.SESSION_URI + "/" + sessionId + LivyConnection.STATEMENT_URI + "/" + stmtId;
        HttpGet request = new HttpGet(url);
        HttpResponse response = executeLivyApi(request);
        checkStatus(response, 200);
        return getRespMap(getResponseAsString(response));
    }

    Map<String, String> openStatement(Configuration actionConf, String sessionId) throws IOException, ParseException {
        String url = getBaseURI(actionConf) + LivyConnection.SESSION_URI + "/" + sessionId + LivyConnection.STATEMENT_URI;
        HttpPost request = new HttpPost(url);
        addReqHeaders(request);
        Map<String, String> params = new HashMap<String, String>();
        params.put("code", actionConf.get(ZeppelinActionExecutor.SRC_CODE));
        request.setEntity(getRequestEntity(params));

        HttpResponse response = executeLivyApi(request);
        checkStatus(response, 201);
        return getRespMap(getResponseAsString(response));
    }

    Map<String, String> openSession(Configuration actionConf) throws IOException, ParseException {
        HttpPost request = new HttpPost(getBaseURI(actionConf) + LivyConnection.SESSION_URI);
        addReqHeaders(request);
        Map<String, String> params = new HashMap<String, String>();
        params.put("kind", actionConf.get(ZeppelinActionExecutor.KIND));
        params.put("proxyUser", actionConf.get(ZeppelinActionExecutor.PROXY_USER));
        request.setEntity(getRequestEntity(params));

        HttpResponse response = executeLivyApi(request);
        checkStatus(response, 201);
        return getRespMap(getResponseAsString(response));
    }

    void checkStatus(HttpResponse response, Integer statusCode) {
        Integer l_statusCode = response.getStatusLine().getStatusCode();
        String errorMessage = response.getStatusLine().getReasonPhrase();
        System.out.println("Status Code: " + statusCode);
        System.out.println("Status Message: " + errorMessage);
        if(l_statusCode.intValue() != statusCode.intValue()) {
            throw new RuntimeException("Error Occurred: Status Code" + statusCode + " Message:" + errorMessage);
        }
    }

    private String getResponseAsString(HttpResponse response) throws IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        String line;
        String outStr = "";
        while ((line = rd.readLine()) != null) {
            System.out.println(line);
            outStr+=line;
        }
        return outStr;
    }

    private Map<String, String> getRespMap(String jsonStr) throws ParseException {
        JSONParser j = new JSONParser();
        JSONObject o = (JSONObject)j.parse(jsonStr);
        Map<String, String> map = new HashMap<String, String>();
        // Common for both session and statement
        if(o.get("id") != null) {
            map.put("id", o.get("id").toString());
            map.put("state", (String) o.get("state"));
        }
        // Statement
        if(o.containsKey("output") && o.get("output") != null) {
            JSONObject out = (JSONObject)j.parse(o.get("output").toString());
            map.put("status", (String) out.get("status"));
            if(!((String) out.get("status")).equalsIgnoreCase("error")) {
                JSONObject out2 = (JSONObject) j.parse(out.get("data").toString());
                map.put("data", out2.toJSONString());
            } else {
                map.put("data", (String) out.get("evalue"));
            }
        }
        // Delete
        if(o.containsKey("msg") && o.get("msg") != null) {
            map.put("msg", (String) o.get("msg"));
        }
        return map;
    }

    private String getBaseURI(Configuration actionConf) {
        return "http://" + actionConf.get(ZeppelinActionExecutor.LIVY_SERVER) + ":" + actionConf.get(ZeppelinActionExecutor.LIVY_SERVER_PORT);
    }

    private StringEntity getRequestEntity(Map<String, String> map) throws IOException {
        StringEntity se = null;
        if(map != null) {
            JSONObject obj = new JSONObject();
            StringWriter out = new StringWriter();
            for(Map.Entry<String, String> entry: map.entrySet()) {
                obj.put(entry.getKey(), entry.getValue());
            }
            obj.writeJSONString(out);
            se = new StringEntity(out.toString());
        }
        return se;
    }

    private HttpResponse executeLivyApi(HttpRequestBase req) throws IOException {
        HttpClient client = new DefaultHttpClient();
        return client.execute(req);
    }

    private void addReqHeaders(HttpRequestBase request) {
        request.addHeader("Content-Type", "application/json");
        request.addHeader("X-Requested-By", "user");
    }
}
