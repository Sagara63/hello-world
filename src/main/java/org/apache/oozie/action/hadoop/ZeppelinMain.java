package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class ZeppelinMain extends LauncherMain {

    public static void main(String[] args) throws Exception {
        run(ZeppelinMain.class, args);
    }

    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Zeppelin action configuration");
        System.out.println("=============================================");

        //loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");
        if (actionXml == null) {
            throw new RuntimeException(
                    "Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file ["
                    + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));

        String errorMsg = validateConfig(actionConf);
        if( errorMsg != null && errorMsg.length() > 0) {
            throw new RuntimeException(errorMsg);
        }
        System.out.println("Action Config is validated successfully.");

        System.out.println("===========Executing Rest API==============");

        try {
            executeSrcCode(actionConf);
        } catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                if (LauncherSecurityManager.getExitCode() != 0) {
                    throw ex;
                }
            }
        }

        System.out.println();
        System.out.println("<<< Invocation of Zeppelin Rest API completed <<<");
        System.out.println();
    }

    private String validateConfig(Configuration actionConf) {
        String errorMsg = "";
        String propValue = actionConf.get(ZeppelinActionExecutor.ZEPPELIN_OUTPUT_PATH);
        if (propValue == null) {
            errorMsg = "Action Configuration does not have " + ZeppelinActionExecutor.ZEPPELIN_OUTPUT_PATH + " property";
        }

        propValue = actionConf.get(ZeppelinActionExecutor.REST_API);
        if (propValue == null) {
            errorMsg = "Action Configuration does not have " + ZeppelinActionExecutor.REST_API + " property";
        }

        propValue = actionConf.get(ZeppelinActionExecutor.SRC_CODE);
        if (propValue == null) {
            errorMsg = "Action Configuration does not have " + ZeppelinActionExecutor.SRC_CODE + " property";
        }

        propValue = actionConf.get(ZeppelinActionExecutor.LIVY_SERVER);
        if (propValue == null) {
            errorMsg = "Action Configuration does not have " + ZeppelinActionExecutor.LIVY_SERVER + " property";
        }

        propValue = actionConf.get(ZeppelinActionExecutor.LIVY_SERVER_PORT);
        if (propValue == null) {
            errorMsg = "Action Configuration does not have " + ZeppelinActionExecutor.LIVY_SERVER_PORT + " property";
        }

        propValue = actionConf.get(ZeppelinActionExecutor.KIND);
        if (propValue == null) {
            errorMsg = "Action Configuration does not have " + ZeppelinActionExecutor.KIND + " property";
        }

        propValue = actionConf.get(ZeppelinActionExecutor.PROXY_USER);
        if (propValue == null) {
            errorMsg = "Action Configuration does not have " + ZeppelinActionExecutor.PROXY_USER + " property";
        }
        return errorMsg;
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
            while (stmtResMap.get("state").equals("running") && count <= 100) {
                Thread.sleep(10000);
                count++;
                stmtResMap = lc.getStatementStatus(actionConf, sesResMap.get("id"), stmtResMap.get("id"));
                if(count == 100 && stmtResMap.get("state").equals("running"))
                    throw new RuntimeException("Time Out exception.");
            }

            // Statement Execution could be completed, Kill the session
            System.out.println("Livy Statement Status:" + stmtResMap.get("state"));
            if (stmtResMap.get("status").equalsIgnoreCase("ok")) {
                // Clean up the session
                data = stmtResMap.get("data");
            }

            // Write output to HDFS
            writeOutput(data, actionConf.get(ZeppelinActionExecutor.ZEPPELIN_OUTPUT_PATH));
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

    private void writeOutput(String data, String zeppelinOutputPath)
            throws Exception {
        Configuration configuration = new Configuration();
        Path outPath = new Path(zeppelinOutputPath);

        BufferedWriter out = null;
        FileSystem fs = null;
        try {
            fs = outPath.getFileSystem(configuration);
            System.out.println("Home Directory:" + fs.getHomeDirectory());
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            fs.mkdirs(outPath);
            Path outFile = new Path(outPath, "zeppelin.out");
            System.out.print("Writing output to :" + outFile);
            out = new BufferedWriter(new OutputStreamWriter(fs.create(outFile),
                    "UTF-8"));
            out.write(data);
            out.flush();

        } finally {
            if (out != null) {
                out.close();
            }
            if (fs != null) {
                fs.close();
            }
        }
    }
}