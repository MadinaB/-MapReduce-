package PasswordCracker;

import static PasswordCracker.PasswordCrackerUtil.findPasswordInRange;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PasswordCrackerMapper
        extends Mapper<Text, Text, Text, Text> {

    //  After reading a key/value, it compute the password by using a function of PasswordCrackerUtil class
    //  If it receive the original password, pass the original password to reducer. Otherwise is not.
    //  FileSystem class : refer to https://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/fs/FileSystem.html

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        String flagFilename = conf.get("terminationFlagFilename");
        FileSystem hdfs = FileSystem.get(conf);
        TerminationChecker terminationChecker = new TerminationChecker(hdfs, flagFilename);

        /** CHECK **/
     //   String candidatePassword=findPasswordInRange(key, value, conf.get("encryptedPassword"), terminationChecker);


        //computes the password by using a function of PasswordCrackerUtil class
        String keyStr= String.valueOf(key);
        String valueStr = String.valueOf(value);
        long rangeBegin = Long.parseLong(keyStr);  //key : rangeStart, value : rangeEnd
        long rangeEnd = Long.parseLong(valueStr) ;  //Long.parseLong(s) ??
        String encryptedPassword = conf.get("encryptedPassword");
        String password = findPasswordInRange(rangeBegin, rangeEnd, encryptedPassword, terminationChecker);


        // If it receive the original password, pass the original password to reducer

        /** CHECK  !!!!! **/
        if(password != null){

        	context.write(new Text(encryptedPassword),new Text(password));
        	terminationChecker.setTerminated();                  // tell after or before?

        	 /** THIS WAS AN ERROR
        	context.write(encryptedPassword, password);          // how to pass to reducer from mapper?
        	**/
        }


    }
}

//  It is class for early termination.
//  In this assignment, a particular file becomes an ealry termination signal.
//  So, If a task find the original password, then the task creates a file using a function in this class.
//  Therefore, tasks will determine whether the quit or not by checking presence of file.
//  FileSystem class : refer to https://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/fs/FileSystem.html

class TerminationChecker {
    FileSystem fs;
    Path flagPath;

    TerminationChecker(FileSystem fs, String flagFilename) {
        this.fs = fs;
        this.flagPath = new Path(flagFilename);
    }

    public boolean isTerminated() throws IOException {
	/** CHECK **/
    	/** ERROR ? **/
    	if(fs.isFile(flagPath)){                                  // tasks will determine whether the quit or not by checking presence of file.
    		return true;
    	}
    	else{
    		return false;
    	}
    	// exists
    	/*
    	 * public boolean isFile(Path f)
               throws IOException
True iff the named path is a regular file.
Note: Avoid using this method. Instead reuse the FileStatus returned by getFileStatus() or listStatus() methods.
*/
    }

    public void setTerminated() throws IOException {
    	/** CHECK **/

    	fs.create(flagPath);										// task creates a file using a function in this class.

    	/*
    	 * create(Path f)
			Create an FSDataOutputStream at the indicated Path.

    	 * */

    }
}
