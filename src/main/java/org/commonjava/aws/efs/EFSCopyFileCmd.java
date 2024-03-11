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
package org.commonjava.aws.efs;

import org.apache.commons.io.IOUtils;
import org.commonjava.aws.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;

@CommandLine.Command( name = "copy" )
public class EFSCopyFileCmd
        implements Runnable
{
    private final Logger logger = LoggerFactory.getLogger( this.getClass() );

    @Option( names = { "-f", "--from" } )
    String source;

    @Option( names = { "-o", "--to" } )
    String target;

    @Option( names = { "-t", "--times" }, defaultValue = "1" )
    int times;

    @Override
    public void run()
    {
        logger.info( "Run copy command" );
        final File sourceFile = Paths.get( source ).toFile();
        final String fileName = sourceFile.getName();
        IntStream.range( 0, times ).forEach( i -> Utils.countDuration( () -> {
            try (FileInputStream in = new FileInputStream( sourceFile );)
            {
                File tempFile = createTempFile( fileName );
                try (FileOutputStream out = new FileOutputStream( tempFile ))
                {
                    IOUtils.copy( in, out );
                    logger.info( "copied file {} to file: {}", source, tempFile );
                }
            }
            catch ( IOException e )
            {
                logger.error( "", e );
            }
        } ) );

    }

    private File createTempFile( String original )
            throws IOException
    {
        File temp = Files.createTempFile( original, ".tmp" ).toFile();
        File newTemp = Paths.get( target, temp.getName() ).toFile();
        newTemp.createNewFile();
        temp.delete();
        return newTemp;
    }
}
