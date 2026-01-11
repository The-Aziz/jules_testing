package org.apache.accumulo.core.client.rfile;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

class FSConfArgs {

  FileSystem fs;
  Configuration conf;

  FileSystem getFileSystem() throws IOException {
    if (fs == null) {
      fs = FileSystem.get(getConf());
    }
    return fs;
  }

  Configuration getConf() throws IOException {
    if (fs != null) {
      return fs.getConf();
    }

    if (conf == null) {
      conf = new Configuration();
    }
    return conf;
  }
}
