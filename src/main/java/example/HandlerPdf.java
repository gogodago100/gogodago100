package example;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Handler value: example.Handler
public class HandlerPdf implements RequestHandler<S3Event, String> {
  Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger logger = LoggerFactory.getLogger(HandlerPdf.class);
  private final String PDF_TYPE = (String) "PDF";
  private final String PDF_MIME = (String) "application/pdf";
  private final String JPG_TYPE = (String) "jpg";
  private final String JPG_MIME = (String) "image/jpeg";

  @Override
  public String handleRequest(S3Event s3event, Context context) {
    PDDocument pdfDoc = null; //Document 생성
    try {
      logger.info("EVENT: " + gson.toJson(s3event));
      S3EventNotificationRecord record = s3event.getRecords().get(0);
      
      String srcBucket = record.getS3().getBucket().getName();

      // Object key may have spaces or unicode non-ASCII characters.
      String srcKey = record.getS3().getObject().getUrlDecodedKey();
      
      //logger.info(">>> srcBucket : " + srcBucket);
      logger.info(">>> srcKey : " + srcKey);

      //String dstBucket = srcBucket;
      String dstBucket = "chosun-convert";
      String dstDir = "IMG/" + srcKey.substring(17,27);
      String dstPdf = srcKey.substring(27,srcKey.length());
      String dstKey = dstDir + dstPdf.replaceAll("PDF", "jpg");
      
      logger.info("dstDir " + dstDir);
      logger.info("dstPdf " + dstPdf);
      logger.info("dstKey " + dstKey);
      
      
      // Infer the image type.
      Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
      if (!matcher.matches()) {
          logger.info("Unable to infer image type for key " + srcKey);
          return "";
      }
      String imageType = matcher.group(1);
      //logger.info(">>> imageType : " + imageType);
      if (!(PDF_TYPE.equals(imageType))) {
          logger.info("Skipping non-image " + srcKey);
          return "NOK";
      } else {
          // Download the image from S3 into a stream
          AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
          S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
          
          InputStream objectData = s3Object.getObjectContent();

          pdfDoc = PDDocument.load(objectData); //Document 생성
          PDFRenderer pdfRenderer = new PDFRenderer(pdfDoc);

          //순회하며 이미지로 변환 처리
          logger.info(">>> version : " + pdfDoc.getVersion());
          //logger.info(">>> .getPages().getCount() : " + pdfDoc.getPages().getCount());
          for (int i=0; i<pdfDoc.getPages().getCount(); i++) {
              //DPI 설정
              BufferedImage bim = pdfRenderer.renderImageWithDPI(i, 10, ImageType.RGB);
              
              // Re-encode image to target format
              ByteArrayOutputStream os = new ByteArrayOutputStream();
              ImageIO.write(bim, "jpg", os);
              
              InputStream is = new ByteArrayInputStream(os.toByteArray());
              // Set Content-Length and Content-Type
              ObjectMetadata meta = new ObjectMetadata();
              meta.setContentLength(os.size());
              meta.setContentType(JPG_MIME);

              // Uploading to S3 destination bucket
              logger.info(">>> Writing to: " + dstBucket + "/" + dstKey);
              try {
                s3Client.putObject(dstBucket, dstKey, is, meta);
              } catch(AmazonServiceException e) {
                logger.error(e.getErrorMessage());
                System.exit(1);
              }
              logger.info(">>> ["+"] Successfully resized " + srcBucket + "/" + srcKey + " and uploaded to " + dstBucket + "/" + dstKey);
          }  // -- for

          pdfDoc.close(); //모두 사용한 PDF 문서는 닫는다.
          return "Ok~~"; 
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
	} finally {
		try {
			if (pdfDoc != null) { pdfDoc.close(); }
		} catch (Exception e) {}
	}
  }
}