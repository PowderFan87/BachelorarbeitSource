package com.hszuesz.logfileanalyzer.recordreader;

import com.hszuesz.logfileanalyzer.Main;
import java.io.IOException;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author Holger Szuesz <it12156@lehre.dhbw-stuttgart.de>
 */
public class LinePatternRecordReader extends RecordReader<Text, IntWritable> {

    private LineReader  objLineReader;
    private long        lngStart;
    private long        lngPosition;
    private long        lngEnd;
    private int         intMaxLineLength;
    private String      strKeyPattern;
    private Pattern     objKeyPattern;
    
    private Text        objKey      = new Text();
    private IntWritable objValue    = new IntWritable(1);
    
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        FileSplit           objSplit            = (FileSplit)is;
        Configuration       objConf             = tac.getConfiguration();
        Path                objFilePath         = objSplit.getPath();
        FileSystem          objFileSystem       = objFilePath.getFileSystem(objConf);
        FSDataInputStream   objFileInputStream  = objFileSystem.open(objFilePath);
        boolean             flgSkipFirstLine    = false;
    
        this.strKeyPattern      = objConf.get("logfileanalyzer.analyzer.keypattern");
        this.objKeyPattern      = Pattern.compile(this.strKeyPattern);
        this.intMaxLineLength   = Integer.MAX_VALUE;
        this.lngStart           = objSplit.getStart();
        this.lngEnd             = this.lngStart + objSplit.getLength();
        
        if (this.lngStart != 0) {
            flgSkipFirstLine = true;
            this.lngStart--;
            objFileInputStream.seek(this.lngStart);
        }
        
        this.objLineReader = new LineReader(objFileInputStream, objConf);
        
        if (flgSkipFirstLine) {
            Text objDummy = new Text();
            
            this.lngStart += this.objLineReader.readLine(objDummy, 0, (int)Math.min((long)Integer.MAX_VALUE, this.lngEnd - this.lngStart));
        }
        
        this.lngPosition = this.lngStart;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        int intNewSize = 0;
        
        while (this.lngPosition < this.lngEnd) {
            intNewSize = this.objLineReader.readLine(this.objKey, this.intMaxLineLength, (int)Math.min((long)Integer.MAX_VALUE, this.lngEnd - this.lngStart));
        
            if (intNewSize == 0) {
                break;
            }
            
            String  strLine     = this.objKey.toString();
            Matcher objMatcher  = this.objKeyPattern.matcher(strLine);
            
            if (objMatcher.find()) {
                this.objKey.set(objMatcher.group(1));
            } else {
                Main.objLogger.log(Level.WARNING, "Can''t find matching key for expression /{0}/ at position {1}. Key is set to 'MISC'", new Object[]{this.strKeyPattern, this.lngPosition});
            
                this.objKey.set("MISC");
            }
            
            this.lngPosition += intNewSize;
            
            if (intNewSize < this.intMaxLineLength) {
                break;
            }
            
            Main.objLogger.log(Level.WARNING, "Line too long ar position {0}", (this.lngPosition - intNewSize));
        }
        
        if (intNewSize == 0) {
            this.objKey     = null;
            this.objValue   = null;
            
            return false;
        } else {
            return true;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.objKey;
    }

    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return this.objValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (this.lngStart == this.lngEnd) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (this.lngPosition - this.lngStart) / (float) (this.lngEnd - this.lngStart));
        }
    }

    @Override
    public void close() throws IOException {
        if (this.objLineReader != null) {
            this.objLineReader.close();
        }
    }
}
