import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class MyS3Client {

	static final String[] BUCKETS = new String[] { "dedupbucket1", "dedupbucket2", "dedupbucket3" };
	static AmazonS3 client;

	public MyS3Client() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/Users/Johnny/.aws/credentials), and is in valid format.", e);
		}
		client = new AmazonS3Client(credentials);
	}

	// Uploads a file to all buckets in our S3
	public void uploadFile(File file) {
		try {
			System.out.println("Uploading: " + file.getName());
			for (String bucket : BUCKETS) {
				client.putObject(new PutObjectRequest(bucket, file.getName(), file));
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	// Downloads file from S3
	public void downloadFile(String fileName, String[] segments) {
		int bucketIndex = 0; // Try with first bucket
		S3Object[] s3Objects = new S3Object[segments.length];
		boolean done = false;
		while (!done) {
			String s3Bucket = BUCKETS[bucketIndex];
			try {
				for (int i = 0; i < segments.length; i++) {
					System.out.println("Downloading: " + segments[i]);
					s3Objects[i] = client.getObject(new GetObjectRequest(s3Bucket, segments[i]));
				}
				constructFile(fileName, s3Objects); // Recompiles the file
				done = true;
			} catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
						+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
				if (bucketIndex == 2) {
					// We failed!
					System.err.println("Failed to get segment from all S3 buckets!");
					System.exit(1);
				} else
					bucketIndex++;
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
						+ "an internal error while trying to " + "communicate with S3, "
						+ "such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
				if (bucketIndex == 2) {
					// We failed!
					System.err.println("Failed to get segment from all S3 buckets!");
					System.exit(1);
				} else
					bucketIndex++;
			} catch (IOException e) {
				System.out.println(e);
			}
		}
	}

	// Takes the S3 segment objects, checks them for length, and creates the
	// file
	private static void constructFile(String fileName, S3Object[] segments) throws IOException {
		PrintWriter pw = new PrintWriter(new File(fileName));
		for (S3Object s3Object : segments) {
			InputStream is = s3Object.getObjectContent();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			int currChar = -1;
			while ((currChar = br.read()) != -1) {
				pw.write(currChar);
			}
			br.close();
			is.close();
		}
		pw.close();
	}

	public void deleteSegment(String segment) {
		try {
			System.out.println("Deleting: " + segment);
			for (String bucket : BUCKETS) {
				client.deleteObject(new DeleteObjectRequest(bucket, segment));
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

}
