/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.store;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentDirectoryPaths {
  public static final String V3_SUBDIRECTORY_NAME = "v3";
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDirectoryPaths.class);


  public static File segmentDirectoryFor(File segmentIndexDirectory, SegmentVersion version) {
    switch(version) {
      case v1:
      case v2:
        return segmentIndexDirectory;
      case v3:
        return new File(segmentIndexDirectory, V3_SUBDIRECTORY_NAME);
      default:
        throw new UnsupportedOperationException("Segment path for version: " + version +
            " and segmentIndexDirectory: " + segmentIndexDirectory + " can not be determined ");
    }
  }

  public static boolean isV3Directory(File path) {
    return path.toString().endsWith(V3_SUBDIRECTORY_NAME);
  }

  public static @Nullable File findMetadataFile(@Nonnull File indexDir) {
    Preconditions.checkNotNull(indexDir);
    Preconditions.checkArgument(indexDir.exists(), "Path: %s to does not exist", indexDir);
    if (! indexDir.isDirectory()) {
      return indexDir;
    }
    File metadataFile = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    if (metadataFile.exists()) {
      return metadataFile;
    }

    File v3Dir = segmentDirectoryFor(indexDir, SegmentVersion.v3);
    metadataFile = new File(v3Dir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    if (metadataFile.exists()) {
      return metadataFile;
    }
    return null;
  }
}
