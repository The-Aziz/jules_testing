package org.apache.accumulo.core.client.rfile;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.accumulo.core.data.Range;

class InputArgs extends FSConfArgs {
    private RFile.InputArguments.FencedPath[] rFiles;
    private RFileSource[] sources;

    InputArgs(String... files) {
        this.rFiles = new RFile.InputArguments.FencedPath[files.length];
        for (int i = 0; i < files.length; i++) {
            this.rFiles[i] = new RFile.InputArguments.FencedPath(new Path(files[i]), new Range());
        }
    }

    InputArgs(RFile.InputArguments.FencedPath... files) {
        this.rFiles = files;
    }

    InputArgs(RFileSource... sources) {
        this.sources = sources;
    }

    RFileSource[] getSources() throws IOException {
        if (sources == null) {
            sources = new RFileSource[rFiles.length];
            for (int i = 0; i < rFiles.length; i++) {
                final Path path = rFiles[i].getPath();
                sources[i] = new RFileSource(getFileSystem().open(path),
                        getFileSystem().getFileStatus(path).getLen(), rFiles[i].getFence());
            }
        } else {
            for (int i = 0; i < sources.length; i++) {
                if (!(sources[i].getInputStream() instanceof FSDataInputStream)) {
                    sources[i] = new RFileSource(new FSDataInputStream(sources[i].getInputStream()),
                            sources[i].getLength(), rFiles[i].getFence());
                }
            }
        }
        return sources;
    }
}
