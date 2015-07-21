package com.hszuesz.logfileanalyzer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 *
 * @author Holger Szuesz <develop@szuesz.de>
 */
public class Configuration extends Properties {

    public static final String CORE_PROPERTIES = "conf/core.properties";
    public static final String DEFAULT_KEY = "DEFAULT_PROPERTIES";
    public static final String LOGGER_KEY = "LOGGER_PROPERTIES";

    /**
     * Default constructor. No user conf given. Use default conf.
     *
     * @throws IOException
     */
    public Configuration() throws IOException {
        super();

        this.init();
        this.setupRuntime();
    }

    /**
     * Constructor overload for passing user conf file.
     *
     * @param strUserConfFile
     * @throws IOException
     */
    public Configuration(String strUserConfFile) throws IOException {
        super();

        this.init();

        try {
            this.load(strUserConfFile);
        } catch (IOException ex) {
            Main.objLogger.log(Level.SEVERE, "Failed to load user properties. Using default propterties instead.", ex);
        }

        this.setupRuntime();
    }

    private void init() throws IOException {
        this.load(CORE_PROPERTIES);
        this.load(this.getProperty(DEFAULT_KEY));
    }

    /**
     * Load properties from file by passed filename
     *
     * @param strPropertiesFilename
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void load(String strPropertiesFilename) throws FileNotFoundException, IOException {
        try (BufferedInputStream objStream = new BufferedInputStream(new FileInputStream(strPropertiesFilename))) {
            this.load(objStream);
        }
    }

    /**
     * Setup runtime before application starts. Setup logger properties from
     * default/user properties and update LogManager.
     * 
     */
    private void setupRuntime() {
        String strRunmode = this.getProperty("RUNMODE");

        this.setProperty("LOGTARGET", MessageFormat.format(this.getProperty("LOGTARGET"), this.getProperty(strRunmode + "_LOGTARGET")));
        this.setProperty("LOGLEVEL", MessageFormat.format(this.getProperty("LOGLEVEL"), this.getProperty(strRunmode + "_LOGLEVEL")));
        this.setProperty("LOGFORMATTER", MessageFormat.format(this.getProperty("LOGFORMATTER"), this.getProperty(strRunmode + "_LOGFORMATTER")));
        this.setProperty("FILEACTION", MessageFormat.format(this.getProperty("FILEACTION"), this.getProperty(strRunmode + "_FILEACTION")));

        Properties objLoggerProperties = new Properties();

        try (BufferedInputStream objStream = new BufferedInputStream(new FileInputStream(this.getProperty(LOGGER_KEY)))) {
            objLoggerProperties.load(objStream);
            
            objLoggerProperties.setProperty("handlers", this.getProperty("LOGTARGET"));
            objLoggerProperties.setProperty("java.util.logging.ConsoleHandler.level", this.getProperty("LOGLEVEL"));
            objLoggerProperties.setProperty("java.util.logging.FileHandler.level", this.getProperty("LOGLEVEL"));
            objLoggerProperties.setProperty("java.util.logging.ConsoleHandler.formatter", this.getProperty("LOGFORMATTER"));
            objLoggerProperties.setProperty("java.util.logging.FileHandler.formatter", this.getProperty("LOGFORMATTER"));
            
            BufferedOutputStream objSave = new BufferedOutputStream(new FileOutputStream(this.getProperty(LOGGER_KEY)));
            
            objLoggerProperties.store(objSave, "UPDATED LOGGER PROPERTIES");

            System.setProperty("java.util.logging.config.file", this.getProperty(LOGGER_KEY));

            LogManager.getLogManager().readConfiguration();
        } catch (IOException ex) {
            Main.objLogger.log(Level.SEVERE, "Failed to read/write Logger properties", ex);
        }
    }
}
