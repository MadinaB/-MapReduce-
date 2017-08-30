/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package PasswordCracker;

import static PasswordCracker.PasswordCrackerUtil.TOTAL_PASSWORD_RANGE_SIZE;

//reads data from a file block, creates equal-sized byte sequences, called split; the splits are of InputSplit type.

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CandidateRangeInputFormat extends InputFormat<Text, Text> {

	private List<InputSplit> splits; // splits are of InputSplit type = equal-sized byte sequences

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new CandidateRangeRecordReader();
    }


    // It generate the splits which are consist of string (or solution space range) and return to JobClient.
    @Override
    // generate candidates for decrypted password  hm
    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
       splits = new ArrayList<>();

        int numberOfSplit = job.getConfiguration().getInt("numberOfSplit", 1);    //get map_count
        long subRangeSize = (TOTAL_PASSWORD_RANGE_SIZE + numberOfSplit - 1) / numberOfSplit;

        /** CHECK  **/

        //  when we suppose the total range size = 8, numberOfSplit = 2,
        //  In one split, candidateRange = "0 3" or "0/3"
        //	In another split, candidateRange = "4 7" or "4/7"


        //generate the splits which are consist of string

        long rangeBegin = 0;
        long rangeEnd = 0;
        for(int i = 0; i < numberOfSplit; i++){
        	rangeEnd = rangeBegin + subRangeSize-1; // excl ]
        	String candidateRange = String.valueOf(rangeBegin)+"_"+String.valueOf(rangeEnd);
        	rangeBegin = rangeEnd+1;  //
            splits.add(new CandidateRangeInputSplit(candidateRange, subRangeSize, null));

            /** ERROR
        	    splits.add(candidateRange);
                DOUBLE CHECK line 75
        	 *  
        	 *  will this work?
        	 *   **/
        }


        return splits;
    }
}
