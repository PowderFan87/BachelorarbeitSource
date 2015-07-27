package com.hszuesz.logfileanalyzer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * {@link Main} class for Logfileanalyzer execution.
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class Main {
    
    /**
     * Standard main methode for jar execution
     * 
     * Main methode for execution of jar file. Checks argument count to match
     * exactly two (input and output directory). Creates instance of
     * {@link LFAConfiguration} class with or without user conf depending on
     * configuration parameter. Calls static init methode of {@link Bootstrap}
     * with configuration to get an Instance of {@link Driver} that will be
     * executed by the Hadoop {@link ToolRunner}.
     * 
     * @param args Array of two string Arguments, first being the input and secound the output directory
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("WRONG PARAMETER COUNT!!!");
            System.out.println("GOOD BYE...");
            
            System.exit(1);
        }
        
        try {
            LFAConfiguration objConf;
            
            //TODO: logfileanalyzer auf lfs ändern damit config überall gleich ist (muss auch in sh script geändert werden)
            if (System.getProperty("logfileanalyzer.userconf") != null) {
                objConf = new LFAConfiguration(System.getProperty("logfileanalyzer.userconf"));
            } else {
                objConf = new LFAConfiguration();
            }
            
            Driver objDriver = Bootstrap.init(objConf);
            
            int lngExitCode = ToolRunner.run(new Configuration(), objDriver, args);
        
            System.exit(lngExitCode);
            
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
