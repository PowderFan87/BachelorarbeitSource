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
import java.util.logging.Logger;

/**
 * Custom Properties class to handle all kind of configurations in propper order
 *
 * Custom extension of {@link Properties} class to handle the three kind of
 * properties files that have to be handled for this application. The first one
 * holds the core properties that can't be overwritten by the user. Secound one
 * holds the defaults that can be altered by the third one (user properties)
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
        this.configurateLogger();
    }

    /**
     * Constructor overload for passing user conf file.
     *
     * @param strUserConfFile Path to user properties file
     * @throws IOException
     */
    public LFAConfiguration(String strUserConfFile) throws IOException {
        super();

        this.init();

        try {
            this.load(strUserConfFile);
        } catch (IOException ex) {
            Logger.getLogger(LFAConfiguration.class.getName()).log(Level.SEVERE, "Failed to load user properties. Using default propterties instead.", ex);
        }

        this.configurateLogger();
    }

    /**
     * Initialize the configuration
     * 
     * First the core properties are laoded into the class. Core properties hold
     * path to default configuration. The key for the default configuration is
     * set in a class constant and used to load the defaults.
     * 
     * @throws IOException 
     */
    private void init() throws IOException {
        this.loadInternal(CORE_PROPERTIES);
        this.loadInternal(this.getProperty(DEFAULT_KEY));
    }

    /**
     * Load properties by filename
     * 
     * Load properties that are not part of the jar build (external files) by
     * reading a file stream.
     *
     * @param strPropertiesFilename name of external file that has to be loaded
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void load(String strPropertiesFilename) throws FileNotFoundException, IOException {
        try (BufferedInputStream objStream = new BufferedInputStream(new FileInputStream(strPropertiesFilename))) {
            this.load(objStream);
        }
    }
    
    /**
     * Load internal properties by filename
     * 
     * Load properties that are part of the jar build and therefor have to be
     * read as a resource stream rather than a file stream.
     * 
     * @param strPropertiesFilename name of resource file that has to be loaded
     * @throws IOException 
     */
    private void loadInternal(String strPropertiesFilename) throws IOException {
        this.load(this.getClass().getResourceAsStream(strPropertiesFilename));
    }

    /**
     * Set final logger configuration and reinitialize {@link LogManager}
     * 
     * The logger properties are set via formatted messages, because the default
     * properties only have {0} placeholders, so that the values can be set by
     * core settings for current runmode. If there are logger settings from an
     * additional user properties file the placeholder is absent and the value
     * stays as it it. The values are then set in a common {@link Properties} class that
     * is initialized by the template logger.properties. The properties object
     * is then converted into a string that can be converted into a
     * ByteArrayInputStream so that the {@link LogManager} can read it as a new Config.
     * 
     * @throws IOException
     */
    private void configurateLogger() throws IOException {
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
