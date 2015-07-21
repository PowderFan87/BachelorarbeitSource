package com.hszuesz.logfileanalyzer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class Main {
    public static final Logger objLogger = Logger.getLogger(Main.class.getName());
    
    /**
     * 
     * @param args 
     */
    public static void main(String[] args) {
        try {
            Configuration objConf;
            
            if (args.length == 1) {
                objConf = new Configuration(args[0]);
            } else {
                objConf = new Configuration();
            }
            
            Bootstrap.init(objConf);
            
            
            
        } catch (IOException ex) {
            objLogger.log(Level.SEVERE, null, ex);
        }
    }
}
