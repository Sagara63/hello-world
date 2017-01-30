package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.jdom.Element;
import org.jdom.Namespace;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

public class ZeppelinActionExecutor extends JavaActionExecutor {

    static final String REST_API = "zeppelin.rest.api.url";
    static final String ZEPPELIN_OUTPUT_PATH = "oozie.zeppelin.output.path";
    private static final String ZEPPELIN_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.ZeppelinMain";
    static final String LIVY_SERVER = "livy.server";
    static final String LIVY_SERVER_PORT = "livy.server.port";
    static final String SRC_CODE = "src.code";
    static final String KIND = "interpreter";
    static final String PROXY_USER = "proxyUser";

    public ZeppelinActionExecutor() {
        super("zeppelin");
    }

    @Override
    public List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        try {
            classes.add(Class.forName(ZEPPELIN_MAIN_CLASS_NAME));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, ZEPPELIN_MAIN_CLASS_NAME);
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        super.kill(context, action);
        LivyConnection lc = new LivyConnection();
        try {
            lc.deleteLivySession(super.getOozieConf(), LivyConnection.LIVY_SESSION_ID);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        } catch (ParseException e) {
            e.printStackTrace(System.out);
        } finally {
            LivyConnection.LIVY_SESSION_ID = null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath) throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();
        actionConf.set(ZEPPELIN_OUTPUT_PATH, actionXml.getChild("output_file_path", ns).getTextTrim());
        actionConf.set(REST_API, actionXml.getChild("restApi", ns).getTextTrim());
        actionConf.set(SRC_CODE, actionXml.getChild("code", ns).getTextTrim());
        actionConf.set(LIVY_SERVER, actionXml.getChild("livyServer", ns).getTextTrim());
        actionConf.set(LIVY_SERVER_PORT, actionXml.getChild("livyServerPort", ns).getTextTrim());
        actionConf.set(KIND, actionXml.getChild("kind", ns).getTextTrim());
        actionConf.set(PROXY_USER, actionXml.getChild("proxyUser", ns).getTextTrim());
        return actionConf;
    }

    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "zeppelin";
    }

}
