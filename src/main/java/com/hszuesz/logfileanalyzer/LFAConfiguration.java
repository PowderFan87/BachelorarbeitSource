package com.hszuesz.logfileanalyzer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 *
 * @author Holger Szuesz <develop@szuesz.de>
 */
public class LFAConfiguration extends Properties {

    public static final String CORE_PROPERTIES  = "/core.properties";
    public static final String DEFAULT_KEY      = "lfa.path.properties.default";
    public static final String LOGGER_KEY       = "lfa.path.properties.logger";

    /**
     * Default constructor. No user conf given. Use default conf.
     *
     * @throws IOException
     */
    public LFAConfiguration() throws IOException {
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
    public LFAConfiguration(String strUserConfFile) throws IOException {
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
        this.loadInternal(CORE_PROPERTIES);
        this.loadInternal(this.getProperty(DEFAULT_KEY));
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
    
    private void loadInternal(String strPropertiesFilename) throws IOException {
        this.load(this.getClass().getResourceAsStream(strPropertiesFilename));
    }

    /**
     * Setup runtime before application starts. Setup logger properties from
     * default/user properties and update LogManager.
     * 
     */
    private void setupRuntime() throws IOException {
        String strRunmodePrefix = "lfa.runmode." + this.getProperty("lfa.runmode").toLowerCase();

        this.setProperty("lfa.logger.handlers", MessageFormat.format(this.getProperty("lfa.logger.handlers"), this.getProperty(strRunmodePrefix + ".logger.handlers")));
        this.setProperty("lfa.logger.level", MessageFormat.format(this.getProperty("lfa.logger.level"), this.getProperty(strRunmodePrefix + ".logger.level")));
        this.setProperty("lfa.logger.formatter", MessageFormat.format(this.getProperty("lfa.logger.formatter"), this.getProperty(strRunmodePrefix + ".logger.formatter")));
        this.setProperty("lfa.fileaction", MessageFormat.format(this.getProperty("lfa.fileaction"), this.getProperty(strRunmodePrefix + ".logger.fileaction")));

        Properties objLoggerProperties = new Properties();
        
        objLoggerProperties.load(this.getClass().getResourceAsStream(this.getProperty(LOGGER_KEY)));

        objLoggerProperties.setProperty("handlers", this.getProperty("lfa.logger.handlers"));
        objLoggerProperties.setProperty("java.util.logging.ConsoleHandler.level", this.getProperty("lfa.logger.level"));
        objLoggerProperties.setProperty("java.util.logging.FileHandler.level", this.getProperty("lfa.logger.level"));
        objLoggerProperties.setProperty("java.util.logging.ConsoleHandler.formatter", this.getProperty("lfa.logger.formatter"));
        objLoggerProperties.setProperty("java.util.logging.FileHandler.formatter", this.getProperty("lfa.logger.formatter"));

        StringWriter objWriter = new StringWriter();

        objLoggerProperties.list(new PrintWriter(objWriter));

        String strLoggerProperties = objWriter.getBuffer().toString();

        InputStream objPropertiesStream = new ByteArrayInputStream(strLoggerProperties.getBytes(StandardCharsets.UTF_8));

        LogManager.getLogManager().readConfiguration(objPropertiesStream);
    }
}
