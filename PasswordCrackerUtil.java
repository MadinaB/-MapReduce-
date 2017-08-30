package PasswordCracker;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

// Utility class for PasswordCracker.

public class PasswordCrackerUtil {
    private static final String PASSWORD_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz";    // Possible Password symbol (NUMBER(0~9) + CHARACTER(A to Z))
    private static final int PASSWORD_LEN = 4;
    public static final long TOTAL_PASSWORD_RANGE_SIZE = (long) Math.pow(PASSWORD_CHARS.length(), PASSWORD_LEN);

    public static MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot use MD5 Library:" + e.getMessage());
        }
    }

    public static String encrypt(String password, MessageDigest messageDigest) {
        messageDigest.update(password.getBytes());
        byte[] hashedValue = messageDigest.digest();
        return byteToHexString(hashedValue);
    }

    public static String byteToHexString(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                builder.append('0');
            }
            builder.append(hex);
        }
        return builder.toString();
    }

    // Tries i'th candidate (rangeBegin <= i < rangeEnd) and compares against encryptedPassword
    // If original password is found, return the password;
    // if not, return null.

    public static String findPasswordInRange(long rangeBegin, long rangeEnd, String encryptedPassword, TerminationChecker checker)
            throws IOException {

    	  /** COMPLETED **/

    	MessageDigest messageDgst = getMessageDigest();
    	String candidate = null;

    	int[] candidateChars=new int[PASSWORD_LEN];
    	transformDecToBase36( rangeBegin, candidateChars);

    	for(long i = rangeBegin; i <= rangeEnd; i++){                    // Tries i'th candidate (rangeBegin <= i < rangeEnd)

    		candidate = transformIntoStr(candidateChars);

    		String encryptedCandidate = encrypt(candidate, messageDgst);

    		if(encryptedCandidate.equals(encryptedPassword)){            //  compares against encryptedPassword
    			return candidate;										 // If original password is found, return the password;
    		}
    		else{
    			candidate = null;
    		}

    		getNextCandidate(candidateChars);
    	}

    	return null;													  // if not, return null.


    }

    /* ###  transformDecToBase36  ###
     * The transformDecToBase36 transforms decimal into numArray that is base 36 number system
     *    */

    private static void transformDecToBase36(long numInDec, int[] numArrayInBase36) {
   
    	long n=numInDec;
        for(int i=0;i<PASSWORD_LEN;i++){
            numArrayInBase36[PASSWORD_LEN-i-1]=(int) n%36;
            n=n/36;
        }

    }

    private static boolean increment(int[] arr, int index) {
    	
            arr[index]=arr[index]+1;
            if(arr[index]<=35){
            	return false;
            }
            else{
                arr[index]=0;
                return true;
            }
    }
    //  ### getNextCandidate ###
    private static void getNextCandidate(int[] arr) {
    
        int i=arr.length-1;
        boolean recurse=increment(arr,i);
        while(recurse){
            if(i==0){break;}
            i=i-1;
            recurse=increment(arr,i);
        }
    }

    private static String transformIntoStr(int[] candidateChars) {
        char[] password = new char[candidateChars.length];
        for (int i = 0; i < password.length; i++) {
            password[i] = PASSWORD_CHARS.charAt(candidateChars[i]);
        }
        return new String(password);
    }
}
