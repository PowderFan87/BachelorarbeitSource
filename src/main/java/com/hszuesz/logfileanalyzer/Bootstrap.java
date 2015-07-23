package com.hszuesz.logfileanalyzer;

import java.util.logging.Level;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class Bootstrap {
    
    public static void init(LFAConfiguration objConfiguration) {
        Main.objLogger.log(Level.INFO, "Start application init via Bootstrap");
        
        Main.objLogger.log(Level.INFO, "CLEANUP: {0}", objConfiguration.getProperty("CLEANUP"));
        
        Main.objLogger.log(Level.INFO, "End application init via Bootstrap");
    }
}
