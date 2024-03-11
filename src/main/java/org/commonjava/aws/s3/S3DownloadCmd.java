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
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

@Command( name = "download" )
public class S3DownloadCmd
        implements Runnable
{
    private final Logger logger = LoggerFactory.getLogger( this.getClass() );

    @Option( names = { "-b", "--bucket" }, required = true )
    String bucketName;

    @Option( names = { "-k", "--key" }, required = true )
    String key;

    @Option( names = { "-s", "--start" } )
    int startIndex;

    @Option( names = { "-e", "--end" } )
    int endIndex;

    @Override
    public void run()
    {
        logger.info( "Run download command" );
        try (S3Client s3 = S3Client.builder().build())
        {
            int start = startIndex;
            int end = endIndex + 1;
            IntStream.range( start, end ).forEach( i -> getObject( s3, bucketName, key + "." + i ) );
        }
    }

    private void getObject( S3Client s3, String bucketName, String objectKey )
    {
        try
        {
            Utils.countDuration( () -> {
                GetObjectRequest objectRequest =
                        GetObjectRequest.builder().key( objectKey ).bucket( bucketName ).build();
                ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes( objectRequest );
                byte[] data = objectBytes.asByteArray();
                try
                {
                    Path temp = Files.createTempFile( objectKey, ".tmp" );
                    try (FileOutputStream os = new FileOutputStream( temp.toFile() ))
                    {
                        os.write( data );
                        logger.info( "download object {} to file: {}", objectKey, temp );
                    }
                }
                catch ( IOException ex )
                {
                    logger.error( "", ex );
                }
            } );
        }
        catch ( S3Exception e )
        {
            logger.error( e.awsErrorDetails().errorMessage() );
        }
    }
}
