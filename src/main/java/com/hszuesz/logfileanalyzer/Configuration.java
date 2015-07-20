package com.hszuesz.logfileanalyzer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Holger Szuesz <develop@szuesz.de>
 */
public class Configuration extends Properties {
    private         String  strUserConfFile;
    private final   String  strDefaultConfFile  = "conf\\defaultConf.properties";
    
    /**
     * Default constructor. No user conf given. Use default conf.
     * 
     * @throws IOException 
     */
    public Configuration() throws IOException {
        super();
        
        this.strUserConfFile = this.strDefaultConfFile;
        
        this.loadProperties();
    }

    /**
     * Constructor overload for passing user conf file.
     * 
     * @param strUserConfFile
     * @throws IOException 
     */
    public Configuration(String strUserConfFile) throws IOException {
        this.strUserConfFile = strUserConfFile;
        
        this.loadProperties();
    }
    
    /**
     * Load properties from file. First load default properties and, if the
     * user properties are not the defaults, override defaults.
     * 
     * @throws FileNotFoundException
     * @throws IOException 
     */
    private void loadProperties() throws FileNotFoundException, IOException {
        this.loadDefaultProperties();
        
        if (!this.strDefaultConfFile.equals(this.strUserConfFile)) {
            try (BufferedInputStream objStream = new BufferedInputStream(new FileInputStream(this.strUserConfFile))) {
                this.load(objStream);
            }
        }
    }
    
    private void loadDefaultProperties() throws FileNotFoundException, IOException {
        try (BufferedInputStream objStream = new BufferedInputStream(new FileInputStream(this.strDefaultConfFile))) {
            this.load(objStream);
        }
    }
}
