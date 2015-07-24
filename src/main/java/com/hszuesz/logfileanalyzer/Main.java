package com.hszuesz.logfileanalyzer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

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
        if (args.length != 2) {
            System.out.println("WRONG PARAMETER COUNT!!!");
            System.out.println("GOOD BYE...");
            
            System.exit(1);
        }
        
        try {
            LFAConfiguration objConf;
            
            if (System.getProperty("logfileanalyzer.userconf") != null) {
                objConf = new LFAConfiguration(System.getProperty("logfileanalyzer.userconf"));
            } else {
                objConf = new LFAConfiguration();
            }
            
            Driver objDriver = Bootstrap.init(objConf);
            
            int lngExitCode = ToolRunner.run(new Configuration(), objDriver, args);
        
            System.exit(lngExitCode);
            
        } catch (IOException ex) {
            objLogger.log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
