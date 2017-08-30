package s3_comp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import com.amazonaws.util.IOUtils;

import java.io.*;
import java.util.*;
import java.awt.image.*;

import javax.imageio.*;
import javax.imageio.stream.ImageOutputStream;

public class compression implements RequestHandler<S3Event, String> {
	
	private static Regions region=Regions.YOUR_REGION;
    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
    ;
    private final String JPG_TYPE = (String) "jpg";
    private final String JPG_MIME = (String) "image/jpeg";
    private final String PNG_TYPE = (String) "png";
    private final String PNG_MIME = (String) "image/png";
    String imageType = "jpg";
    public compression() {}

    // Test purpose only.
    /*compression(AmazonS3 s3) {
        this.s3 = s3;
    }*/

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        
        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        Connection con =null;
    	String output = "";
    	String qn = "insert into lamda (firname,age) values ('"+bucket+"','"+key+"')";
    	int i = 0;
    	
    		
        try {
        	
			
			S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
            
			
			
			S3ObjectInputStream objectContent = response.getObjectContent();
           BufferedImage image = ImageIO.read(objectContent);
		      
		     
		      Iterator<ImageWriter>writers =  ImageIO.getImageWritersByFormatName("jpg");
		      ImageWriter writer = (ImageWriter) writers.next();
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("CONNECTION_TO_YOUR_RDS");
			Statement s = con.createStatement();
			i = s.executeUpdate(qn);
			context.getLogger().log("ahia sudhi thai gayu");
			
			
			
            
			
		      
		      ByteArrayOutputStream os = new ByteArrayOutputStream();
		      ImageOutputStream ios = ImageIO.createImageOutputStream(os);
		      
		      writer.setOutput(ios);

		      ImageWriteParam param = writer.getDefaultWriteParam();
		      
		      param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
		      param.setCompressionQuality(0.5f);
		      writer.write(null, new IIOImage(image, null, null), param);
		      ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());

	            //InputStream is = new ByteArrayInputStream(retValue);
	            BufferedImage destImage = ImageIO.read(is);

	             ByteArrayOutputStream os1 = new ByteArrayOutputStream();
	             ImageIO.write(destImage, imageType, os1);
	             InputStream is1 = new ByteArrayInputStream(os1.toByteArray()); 


	             // Set Content-Length and Content-Type
	             ObjectMetadata meta = new ObjectMetadata();
	             meta.setContentLength(os1.size());
	             System.out.println(os1.size());
	             if (JPG_TYPE.equals(imageType)) {
	                 meta.setContentType(JPG_MIME);
	             }
	             if (PNG_TYPE.equals(imageType)) {
	                 meta.setContentType(PNG_MIME);
	             }

	             // Uploading to S3 destination bucket
	             //.out.println("Writing to: " + dstBucket + "/" + dstKey);
	             s3.putObject("BUCKETNAME", key, is1, meta);
		     // s3client.putObject(new PutObjectRequest("compre2", key, compressedImageFile));
		      
			
            String contentType = response.getObjectMetadata().getContentType();
            context.getLogger().log("CONTENT TYPE: " + contentType);
            return contentType+key;
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", bucket, key));
            return "error";
        }
    }
}
