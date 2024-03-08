/**
 * Copyright (C) 2024 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commonjava.aws.s3;

import org.commonjava.aws.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.IoUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Command( name = "upload" )
public class S3UploadCmd
        implements Runnable
{
    private final Logger logger = LoggerFactory.getLogger( this.getClass() );

    @Option( names = { "-b", "--bucket" }, required = true )
    String bucketName;

    @Option( names = { "-f", "--file" }, required = true )
    String filePath;

    @Option( names = { "-t", "--times" }, defaultValue = "1" )
    int times;

    @Override
    public void run()
    {
        S3Client s3 = S3Client.builder().build();
        putS3Object( s3, bucketName, filePath, times );
        s3.close();
    }

    private void putS3Object( S3Client s3, String bucketName, String filePath, int uploadTimes )
    {
        try
        {
            String fileName = Paths.get( filePath ).getFileName().toString();
            IntStream.range( 0, uploadTimes ).forEach( i -> Utils.countDuration( () -> {
                Map<String, String> metadata = new HashMap<>();
                metadata.put( "x-amz-meta-myVal", "test" );
                final String key = fileName + "." + i;
                try (FileInputStream fileIn = new FileInputStream( filePath );
                     S3OutputStream s3os = new S3OutputStream( s3, bucketName, key, metadata ))
                {
                    IoUtils.copy( fileIn, s3os );
                    logger.info( "Uploaded {} from file {}", key, filePath );
                }
                catch ( IOException ex )
                {
                    logger.error( "Failed to upload file {} due to error: {}", filePath, ex.getMessage() );
                }
                //                PutObjectRequest putOb =
                //                        PutObjectRequest.builder().bucket( bucketName ).key( key ).metadata( metadata ).build();
                //                logger.info( "Upload {} from file {}", key, filePath );
                //                s3.putObject( putOb, RequestBody.fromFile( new File( filePath ) ) );
            } ) );

        }
        catch ( S3Exception e )
        {
            logger.error( e.getMessage() );
        }
    }
}
