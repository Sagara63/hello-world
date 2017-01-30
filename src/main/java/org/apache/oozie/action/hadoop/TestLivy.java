package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.oozie.action.hadoop.ZeppelinActionExecutor.*;

/**
 * Created by nnr0005 on 1/24/17.
 */
public class TestLivy {
    public static void main(String []args) throws Exception {

        Configuration actionConf = new Configuration();
        actionConf.set(ZEPPELIN_OUTPUT_PATH, "hdfs://sandbox.hortonworks.com:8020/zeppelin_output");
        actionConf.set(LIVY_SERVER, "sandbox.hortonworks.com");
        actionConf.set(LIVY_SERVER_PORT, "8998");
        actionConf.set(KIND, "spark");
        actionConf.set(PROXY_USER, "user");
        actionConf.set(SRC_CODE, "val a = 10");

        new TestLivy().executeSrcCode(actionConf);

    }

    private void executeSrcCode(Configuration actionConf) throws Exception {
        String data = "";
        // Open Livy Session
        LivyConnection lc = new LivyConnection();
        Map<String, String> stmtResMap = new HashMap<String, String>();
        Map<String, String> sesResMap = lc.openSession(actionConf);
        System.out.println("Livy Session is opened, Session Id:" + sesResMap.get("id"));
        LivyConnection.LIVY_SESSION_ID = sesResMap.get("id");
        try {

            // Check Session Status
            Integer count = 0;
            while (sesResMap.get("state").equals("starting") && count < 10) {
                Thread.sleep(10000);
                count++;
                sesResMap = lc.getSessionStatus(actionConf, sesResMap.get("id"));
            }

            // Open Livy Statement
            stmtResMap = lc.openStatement(actionConf, sesResMap.get("id"));
            System.out.println("Livy Statement is created, statement Id:" + stmtResMap.get("id"));

            // Check Statement Status
            count = 0;
            while (stmtResMap.get("state").equals("running") && count < 10) {
                Thread.sleep(10000);
                count++;
                stmtResMap = lc.getStatementStatus(actionConf, sesResMap.get("id"), stmtResMap.get("id"));
            }

            // Statement Execution could be completed, Kill the session
            System.out.println("Livy Statement Status:" + stmtResMap.get("state"));
            if (stmtResMap.get("status").equalsIgnoreCase("ok")) {
                // Clean up the session
                data = stmtResMap.get("data");
            }

            // Write output to HDFS
            //writeOutput(data, actionConf.get(ZeppelinActionExecutor.ZEPPELIN_OUTPUT_PATH));
            System.out.println("Output is written to HDFS successfully.");

        } finally {
            // clean up session
            stmtResMap = lc.deleteLivySession(actionConf, LivyConnection.LIVY_SESSION_ID);
            if (stmtResMap.get("msg").equalsIgnoreCase("deleted")) {
                System.out.println("Session deleted successfully.");
                LivyConnection.LIVY_SESSION_ID = null;
            }
        }
    }
}
