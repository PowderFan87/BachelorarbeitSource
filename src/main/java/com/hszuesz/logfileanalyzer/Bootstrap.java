package com.hszuesz.logfileanalyzer;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ToolRunner;

/**
 * {@link Bootstrap} class for creating a new {@link Driver} via init methode
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class Bootstrap {
    
    /**
     * Create and configure instance of {@link Driver}
     * 
     * Create a new instance of the {@link Driver} class with the configuration
     * stored in the passed {@link LFAConfiguration} instance. Setting up the
     * relevant classes to use for the driver when executed by the {@link ToolRunner}
     * After that all keys are iterated and additional configuration values are
     * passed to the {@link Driver}.
     * 
     * @param objConfiguration Instance of configuration that has to be used to setup dirver
     * @return configured {@link Driver}
     */
    public static Driver init(LFAConfiguration objConfiguration) {
        Logger.getLogger(Bootstrap.class.getName()).log(Level.INFO, "START: application init via Bootstrap");
        
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
            
            Set<String> setKeys = objConfiguration.stringPropertyNames();
            
            for (String strKey : setKeys) {
                if (strKey.contains("lfa.mapper.") || strKey.contains("lfa.reducer.") || strKey.contains("lfa.driver.add.")) {
                    objDriver.setAdditionalConfiguration(strKey, objConfiguration.getProperty(strKey));
                }
            }
            
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Bootstrap.class.getName()).log(Level.SEVERE, null, ex);
            
            return null;
        }
        
        Logger.getLogger(Bootstrap.class.getName()).log(Level.INFO, "END: application init via Bootstrap");
        
        return objDriver;
    }
}
