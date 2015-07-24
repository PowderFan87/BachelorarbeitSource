package com.hszuesz.logfileanalyzer;

import java.util.logging.Level;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class Bootstrap {
    
    public static Driver init(LFAConfiguration objConfiguration) {
        Main.objLogger.log(Level.INFO, "Start application init via Bootstrap");
        
        Driver objDriver;
        
        try {
            objDriver = new Driver();
            
            objDriver.setClsInputFormatClass(Class.forName(objConfiguration.getProperty("lfa.driver.input.format")));
            
            objDriver.setClsOutputFormatClass(Class.forName(objConfiguration.getProperty("lfa.driver.output.format")));
            objDriver.setClsOutputKeyClass(Class.forName(objConfiguration.getProperty("lfa.driver.output.key")));
            objDriver.setClsOutputValueClass(Class.forName(objConfiguration.getProperty("lfa.driver.output.value")));
            
            objDriver.setClsMapperClass(Class.forName(objConfiguration.getProperty("lfa.driver.mapper")));
            objDriver.setClsReducerClass(Class.forName(objConfiguration.getProperty("lfa.driver.reducer")));
            
            objDriver.setStrJobName(objConfiguration.getProperty("lfa.driver.job.name"));
            
        } catch (ClassNotFoundException ex) {
            Main.objLogger.log(Level.SEVERE, null, ex);
            
            return null;
        }
        
        Main.objLogger.log(Level.INFO, "End application init via Bootstrap");
        
        return objDriver;
    }
}
