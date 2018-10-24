package com.takipi.oss.storage.fs.gcs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.takipi.oss.storage.data.simple.SimpleSearchResponse;
import com.takipi.oss.storage.fs.BaseRecord;
import com.takipi.oss.storage.fs.Record;
import com.takipi.oss.storage.fs.api.Filesystem;
import com.takipi.oss.storage.fs.api.SearchRequest;
import com.takipi.oss.storage.fs.api.SearchResult;
import com.takipi.oss.storage.helper.FilesystemUtil;

public class GCSFilesystem<T extends BaseRecord> implements Filesystem<T> {

    private final Storage storage;
    private final String bucket;
    private final String pathPrefix;
    
    public GCSFilesystem(Storage storage, String bucket, String pathPrefix) {
        
        this.storage = storage;
        this.bucket = bucket;
        this.pathPrefix = pathPrefix;
    }

    @Override
    public void put(T record, InputStream is) throws IOException {
        BlobId blobId = BlobId.of(bucket, keyOf(record));
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
        Blob blob = storage.create(blobInfo, is);
    }

    @Override
    public InputStream get(T record) throws IOException {
        BlobId blobId = BlobId.of(bucket, keyOf(record));
        return Channels.newInputStream(storage.reader(blobId));
    }

    @Override
    public void delete(T record) throws IOException {
        storage.delete(bucket, keyOf(record));
    }

    @Override
    public boolean exists(T record) throws IOException {
        return storage.get(bucket, keyOf(record)).exists();
    }

    @Override
    public long size(T record) throws IOException {
        return storage.get(bucket, keyOf(record)).getSize();
    }

    @Override
    public SearchResult search(SearchRequest searchRequest) throws IOException {
        String directory;
        Blob resultBlob = null;
        String searchName, resultPath=null;

        if (this.pathPrefix != null) {
            directory = this.pathPrefix + "/" + searchRequest.getBaseSearchPath();
        } else {
            directory = searchRequest.getBaseSearchPath();
        }

        Page<Blob> blobs = storage.list(bucket, BlobListOption.prefix(directory));
        searchName=searchRequest.getName();
        for (Blob blob : blobs.iterateAll()) {
            String blogName=blob.getName();
            if (blob.getName().contains(searchRequest.getName())) {
                resultBlob = blob;
                resultPath = pathPrefix != null ? 
                        blogName.replaceFirst("^" + pathPrefix + "/", "") : 
                        blogName;
                break;
            }
        }

        if (resultBlob == null) {
            return null;
        } else {
            String data = FilesystemUtil.encode(Channels.newInputStream(resultBlob.reader()),
                    searchRequest.getEncodingType());
            return new SimpleSearchResponse(data, resultPath);
        }
    }

    @Override
    public boolean healthy() {
        return true;
    }

    @Override
    public BaseRecord pathToRecord(String path) {
        String[] strs = path.trim().split(File.separator,3);
        if (strs.length == 3)
            return Record.newRecord(strs[0], strs[1], strs[2]);
        else
            return null;
    }
    
    private String keyOf(T record) {
        if (this.pathPrefix != null) {
            return this.pathPrefix + File.separator + record.getPath();
        }
        
        return record.getPath();
    }
}
