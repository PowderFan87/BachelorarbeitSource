package com.hszuesz.logfileanalyzer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        try {
            LFAConfiguration objConf = new LFAConfiguration();
            
            Bootstrap.init(objConf);
            
            int lngExitCode = ToolRunner.run(new org.apache.hadoop.conf.Configuration(), new Quicktest(), args);
        
            System.exit(lngExitCode);
            
        } catch (IOException ex) {
            objLogger.log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
