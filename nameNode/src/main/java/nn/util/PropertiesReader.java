package nn.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

public class PropertiesReader {
    public static Properties PROPERTIES;
    private static String PROPERTY_FILE_PATH = "conf/dataNodeConfig.properties";
    private static final Logger LOGGER = Logger.getLogger(PropertiesReader.class.getName());
    static{
        PROPERTIES = new Properties();
        try {
            PROPERTIES.load(new FileInputStream(PROPERTY_FILE_PATH));
        }catch (IOException e2) {
            LOGGER.warning("Fail to read dataNodeConfig.properties in " + PROPERTIES + " . Data node won't start.");
        }
    }

    public static String getPropertyAsString(String propertyName){
        try {
            return PROPERTIES.get(propertyName).toString();
        }catch (NullPointerException e){
            LOGGER.warning("No such property: " + propertyName);
        }
        return null;
    }

    public static int getPropertyAsInt(String propertyName){
        int property = -1;
        try{
            property = Integer.parseInt(PROPERTIES.getProperty(propertyName).toString());
        }catch (NumberFormatException e){
            LOGGER.warning("Name node server port must be integer");
        }catch (NullPointerException e){
            LOGGER.warning("No such property: " + propertyName);
        }
        return property;
    }
}
