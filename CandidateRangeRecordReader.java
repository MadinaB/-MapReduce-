package PasswordCracker;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

// takes splits as input and creates records of key/value pairs;

//These splits are transformed into records of key/value pairs representing a solution sub-space range
//(key : rangeStart, value : rangeEnd) in CandidateRangeRecordReader class.


public class CandidateRangeRecordReader extends RecordReader<Text, Text> {
    private String rangeBegin;
    private String rangeEnd;
    private boolean done = false;

    CandidateRangeRecordReader() {

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(rangeBegin);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(rangeEnd);
    }

    // After creating this class, It is called with a inputSplit as a parameter. and It divides inputSplit by a record of key/value.
    // inputSplit is String candidateRange = String.valueOf(rangeBegin)+"_"+String.valueOf(rangeEnd);

    /*
     * 	String s = "12,23";
		String[] array = s.split(",");
		Text t1 = new Text(array[0]);
		Text t2 = new Text(array[1]);
     * */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        /** CHECK **/
        CandidateRangeInputSplit candidataRangeSplit = (CandidateRangeInputSplit) split;

        String candidateRange = String.valueOf(candidataRangeSplit.getInputRange());
        String[] candidateRangeBeginEnd = candidateRange.split("_");

        rangeBegin = candidateRangeBeginEnd[0];    //parse to string
        rangeEnd = candidateRangeBeginEnd[1];      //parse to string


    }

    // Normally, this function in the RecordReader is called repeatedly to polulate the key and value objects for the mapper.
    // and When the reader gets to the end of the stream, the next method false, and the map task completes.

    // But here is called only one.

    /*	Read the next key, value pair.
	Returns:
		true if a key/value pair was read*/

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
    
    	if(done == true){
    		return false;
    	}
    	else{
            done = true;
            return true;
    	}

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (done) {
            return 1.0f;
        }
        return 0.0f;
    }

    @Override
    public void close() throws IOException {
    }
}
