package rajesh.dtc.server.config.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import rajesh.dtc.server.config.ServerConfig;
import rajesh.dtc.server.config.json.JsonServerConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by rajesh on 2/20/16.
 */
public class ServerConfigFactory {

    public static ServerConfig fromJson(File file) throws FileNotFoundException {
        FileReader fr = new FileReader(file);
        Gson gson = new GsonBuilder().create();
        JsonServerConfig jsonServerConfig = gson.fromJson(fr, JsonServerConfig.class);
        return jsonServerConfig;

    }
}
