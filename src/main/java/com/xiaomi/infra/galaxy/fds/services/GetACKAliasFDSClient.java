package com.xiaomi.infra.galaxy.fds.services;

import com.xiaomi.infra.galaxy.fds.client.FDSClientConfiguration;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDS;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.credential.GalaxyFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectSummary;

import java.io.IOException;

public class GetACKAliasFDSClient {
    //
//  access_key="5951744265936"
//  access_secret="r63EXucC6ndd599fvW4mAg=="
//  bucket_name="content-resources"
//  object_name="yidian_user_imeimd5/yidian_user_imeimd5.txt"
    private static String APP_ACCESS_KEY = "5951744265936";
    private static String APP_ACCESS_SECRET = "r63EXucC6ndd599fvW4mAg==";

    private static String BUCKET_NAME = "yidianzixun";
    private static String OBJECT_NAME = "push/push_alias/data";

    public static void main(String[] args)
            throws GalaxyFDSClientException, IOException {

//        if (args.length < 4 ) {
//            System.out.println(args.length);
//            System.err.println("Usage java %s <APP_ACCESS_KEY> <APP_ACCESS_SECRET> <BUCKET_NAME> <OBJECT_NAME>");
//            System.exit(0);
//        }
        if (args.length >= 4) {
            APP_ACCESS_KEY = args[0];
            APP_ACCESS_SECRET = args[1];
            BUCKET_NAME = args[2];
            OBJECT_NAME = args[3];
        }
        GalaxyFDSCredential credential = new BasicFDSCredential(
                APP_ACCESS_KEY, APP_ACCESS_SECRET);

        // Construct the GalaxyFDSClient object.
        FDSClientConfiguration fdsConfig = new FDSClientConfiguration();
        fdsConfig.enableHttps(false);
        fdsConfig.enableCdnForUpload(false);
        fdsConfig.enableCdnForDownload(false);
        GalaxyFDS fdsClient = new GalaxyFDSClient(credential, fdsConfig);


        // Get the object
        //FDSObject object = fdsClient.getObject(BUCKET_NAME, OBJECT_NAME);
        //  FDSObjectMetadata metadata = object.getObjectMetadata();
        FDSObjectListing objectListing = fdsClient.listObjects(BUCKET_NAME, OBJECT_NAME);

        objectListing.getObjectSummaries().get(0).getObjectName();
        for (FDSObjectSummary fdsObjectSummary : objectListing.getObjectSummaries()) {
            String objectName = fdsObjectSummary.getObjectName();
            System.out.println(objectName);
//            FDSObject fdsObject = fdsClient.getObject(BUCKET_NAME, objectName);
//            // Read the object content
//            FDSObjectInputStream in = object.getObjectContent();
//            byte[] buffer = new byte[1024];
//            int totalReadLen = 0;
//            int readLen = 0;
//            while ((readLen = in.read(buffer)) > 0) {
//                System.out.print(readLen);
//                System.out.println(new String(buffer));
//            }
//            in.close();
        }


        // Read the object content
//        FDSObjectInputStream in = object.getObjectContent();
//        byte[] buffer = new byte[1024];
//        int totalReadLen = 0;
//        int readLen = 0;
//        while ((readLen = in.read(buffer)) > 0) {
//            System.out.print(readLen);
//            System.out.println(new String(buffer));
//        }
//        in.close();

        // Delete the object
        //fdsClient.deleteObject(BUCKET_NAME, OBJECT_NAME);

        // Delete the bucket
        //fdsClient.deleteBucket(BUCKET_NAME);
    }
}

