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

import org.apache.commons.lang3.StringUtils;
import org.commonjava.aws.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.stream.IntStream;

@Command( name = "list" )
public class S3ListCmd
        implements Runnable
{
    private final Logger logger = LoggerFactory.getLogger( this.getClass() );

    @CommandLine.Option( names = { "-b", "--bucket" }, required = true )
    String bucketName;

    @CommandLine.Option( names = { "-f", "--folder" }, defaultValue = "/" )
    String folder;

    @CommandLine.Option( names = { "-t", "--times" }, defaultValue = "1" )
    int times;

    @Override
    public void run()
    {
        logger.info( "Run list command" );
        S3Client s3 = S3Client.builder().build();
        IntStream.range( 0, times ).forEach( i -> listBucketObjects( s3, bucketName, folder ) );
        s3.close();
    }

    private void listBucketObjects( S3Client s3, String bucketName, String folder )
    {
        try
        {
            Utils.countDuration( () -> {
                ListObjectsV2Request.Builder listObjectsBuilder =
                        ListObjectsV2Request.builder().bucket( bucketName ).delimiter( "/" );
                if ( StringUtils.isNotBlank( folder ) && !"/".equals( folder.trim() ) )
                {
                    listObjectsBuilder.prefix( folder );
                }
                ListObjectsV2Response res = s3.listObjectsV2( listObjectsBuilder.build() );
                List<S3Object> objects = res.contents();
                for ( S3Object myValue : objects )
                {
                    logger.info( "The name of the key is {}", myValue.key() );
                    logger.info( "The object is {}", String.format( "%s KBs", calKb( myValue.size() ) ) );
                    logger.info( "The owner is {}", myValue.owner() );
                }
            } );
        }
        catch ( S3Exception e )
        {
            logger.error( e.awsErrorDetails().errorMessage() );
        }
    }

    // convert bytes to kbs.
    private long calKb( Long val )
    {
        return val / 1024;
    }
}
