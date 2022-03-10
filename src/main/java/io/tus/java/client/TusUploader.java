package io.tus.java.client;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * This class is used for doing the actual upload of the files. Instances are returned by
 * {@link TusClient#createUpload(TusUpload)}, {@link TusClient#createUpload(TusUpload)} and
 * {@link TusClient#resumeOrCreateUpload(TusUpload)}.
 * <br>
 * After obtaining an instance you can upload a file by following these steps:
 * <ol>
 *  <li>Upload a chunk using {@link #uploadChunk()}</li>
 *  <li>Optionally get the new offset ({@link #getOffset()} to calculate the progress</li>
 *  <li>Repeat step 1 until the {@link #uploadChunk()} returns -1</li>
 *  <li>Close HTTP connection and InputStream using {@link #finish()} to free resources</li>
 * </ol>
 */
public class TusUploader {
    private final URL uploadURL;
    private final TusInputStream input;
    private long offset;
    private final TusClient client;
    private final TusUpload upload;
    private byte[] buffer;
    private int requestPayloadSize = 10 * 1024 * 1024;
    private boolean requestInProgress = false;

    /**
     * Begin a new upload request by opening a PATCH request to specified upload URL. After this
     * method returns a connection will be ready and you can upload chunks of the file.
     *
     * @param client    Used for preparing a request ({@link TusClient#prepareConnection(HttpURLConnection)}
     * @param upload    {@link TusUpload} to be uploaded.
     * @param uploadURL URL to send the request to
     * @param input     Stream to read (and seek) from and upload to the remote server
     * @param offset    Offset to read from
     * @throws IOException Thrown if an exception occurs while issuing the HTTP request.
     */
    public TusUploader(TusClient client, TusUpload upload, URL uploadURL, TusInputStream input, long offset)
            throws IOException {
        this.uploadURL = uploadURL;
        this.input = input;
        this.offset = offset;
        this.client = client;
        this.upload = upload;

        input.seekTo(offset);

        setChunkSize(2 * 1024 * 1024);
    }

    /**
     * Sets the used chunk size. This number is used by {@link #uploadChunk()} to indicate how
     * much data is uploaded in a single take. When choosing a value for this parameter you need to
     * consider that uploadChunk() will only return once the specified number of bytes has been
     * sent. For slow internet connections this may take a long time. In addition, a buffer with
     * the chunk size is allocated and kept in memory.
     *
     * @param size The new chunk size
     */
    public void setChunkSize(int size) {
        buffer = new byte[size];
    }

    /**
     * Returns the current chunk size set using {@link #setChunkSize(int)}.
     *
     * @return Current chunk size
     */
    public int getChunkSize() {
        return buffer.length;
    }

    /**
     * Set the maximum payload size for a single request counted in bytes. This is useful for splitting
     * bigger uploads into multiple requests. For example, if you have a resource of 2MB and
     * the payload size set to 1MB, the upload will be transferred by two requests of 1MB each.
     * <p>
     * The default value for this setting is 10 * 1024 * 1024 bytes (10 MiB).
     * <p>
     * Be aware that setting a low maximum payload size (in the low megabytes or even less range) will result in
     * decreased performance since more requests need to be used for an upload. Each request will come with its overhead
     * in terms of longer upload times.
     * <p>
     * Be aware that setting a high maximum payload size may result in a high memory usage since
     * tus-java-client usually allocates a buffer with the maximum payload size (this buffer is used
     * to allow retransmission of lost data if necessary). If the client is running on a memory-
     * constrained device (e.g. mobile app) and the maximum payload size is too high, it might
     * result in an {@link OutOfMemoryError}.
     * <p>
     * This method must not be called when the uploader has currently an open connection to the
     * remote server. In general, try to set the payload size before invoking {@link #uploadChunk()}
     * the first time.
     *
     * @param size Number of bytes for a single payload
     * @throws IllegalStateException Thrown if the uploader currently has a connection open
     * @see #getRequestPayloadSize()
     */
    public void setRequestPayloadSize(int size) throws IllegalStateException {
        if (requestInProgress) {
            throw new IllegalStateException("payload size for a single request must not be "
                    + "modified as long as a request is in progress");
        }

        requestPayloadSize = size;
    }

    /**
     * Get the current maximum payload size for a single request.
     *
     * @return Number of bytes for a single payload
     * @see #setChunkSize(int)
     */
    public int getRequestPayloadSize() {
        return requestPayloadSize;
    }

    /**
     * Upload a part of the file by reading a chunk from the InputStream and writing
     * it to the HTTP request's body. If the number of available bytes is lower than the chunk's
     * size, all available bytes will be uploaded and nothing more.
     * No new connection will be established when calling this method, instead the connection opened
     * in the previous calls will be used.
     * The size of the read chunk can be obtained using {@link #getChunkSize()} and changed
     * using {@link #setChunkSize(int)}.
     * In order to obtain the new offset, use {@link #getOffset()} after this method returns.
     *
     * @return Number of bytes read and written.
     * @throws IOException Thrown if an exception occurs while reading from the source or writing
     *                     to the HTTP request.
     */
    public int uploadChunk() throws IOException, ProtocolException {
        requestInProgress = true;
        int bytesRemainingForRequest = requestPayloadSize;
        OkHttpClient okHttpClient = client.getOrCreateOkHttpClient();

        Request.Builder requestBuilder = client.getRequestBuilderWithHeaders()
                .url(uploadURL);
        requestBuilder.header("Upload-Offset", Long.toString(offset));
        requestBuilder.header("Content-Type", "application/offset+octet-stream");
        requestBuilder.header("Expect", "100-continue");


        int bytesToRead = Math.min(getChunkSize(), bytesRemainingForRequest);
        int bytesRead = input.read(buffer, bytesToRead);

        if (bytesRead == -1) {
            // No bytes were read since the input stream is empty
            return -1;
        }

        requestBuilder.patch(RequestBody.create(MediaType.get("application/offset+octet-stream"), buffer));

        Response response = okHttpClient.newCall(requestBuilder.build()).execute();

        offset += bytesRead;
        bytesRemainingForRequest -= bytesRead;

        if (bytesRemainingForRequest <= 0) {
            finishConnection(response);
        }

        requestInProgress = false;
        return bytesRead;
    }

    /**
     * Get the current offset for the upload. This is the number of all bytes uploaded in total and
     * in all requests (not only this one). You can use it in conjunction with
     * {@link TusUpload#getSize()} to calculate the progress.
     *
     * @return The upload's current offset.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * This methods returns the destination {@link URL} of the upload.
     *
     * @return The {@link URL} of the upload.
     */
    public URL getUploadURL() {
        return uploadURL;
    }

    /**
     * Finish the request by closing the HTTP connection and the InputStream.
     * You can call this method even before the entire file has been uploaded. Use this behavior to
     * enable pausing uploads.
     * This method is equivalent to calling {@code finish(false)}.
     *
     * @throws ProtocolException Thrown if the server sends an unexpected status
     *                           code
     * @throws IOException       Thrown if an exception occurs while cleaning up.
     */
    public void finish() throws ProtocolException, IOException {
        finish(true);
    }

    /**
     * Finish the request by closing the HTTP connection. You can choose whether to close the InputStream or not.
     * You can call this method even before the entire file has been uploaded. Use this behavior to
     * enable pausing uploads.
     * Be aware that it doesn't automatically release local resources if {@code closeStream == false} and you do
     * not close the InputStream on your own. To be safe use {@link TusUploader#finish()}.
     *
     * @param closeInputStream Determines whether the InputStream is closed with the HTTP connection. Not closing the
     *                         Input Stream may be useful for future upload a future continuation of the upload.
     * @throws IOException Thrown if an exception occurs while cleaning up.
     */
    public void finish(boolean closeInputStream) throws IOException {
        if (upload.getSize() == offset) {
            client.uploadFinished(upload);
        }

        // Close the TusInputStream after checking the response and closing the connection to ensure
        // that we will not need to read from it again in the future.
        if (closeInputStream) {
            input.close();
        }
    }

    /**
     * @param response - server response for chunk uploading
     * @throws ProtocolException unexpected response code or invalid upload offset
     */
    private void finishConnection(Response response) throws ProtocolException {

        int responseCode = response.code();

        if (!(responseCode >= 200 && responseCode < 300)) {
            throw new ProtocolException("unexpected status code (" + responseCode + ") while uploading chunk");
        }

        // TODO detect changes and seek accordingly
        long serverOffset = getHeaderFieldLong(response, "Upload-Offset");
        if (serverOffset == -1) {
            throw new ProtocolException("response to PATCH request contains no or invalid Upload-Offset header");
        }
        if (offset != serverOffset) {
            throw new ProtocolException(
                    String.format(
                            "response contains different Upload-Offset value (%d) than expected (%d)",
                            serverOffset, offset)
            );
        }
    }

    /**
     * @param response - response of the http-request
     * @param field - header name
     * @return long value of header field in response
     */
    private long getHeaderFieldLong(Response response, @SuppressWarnings("SameParameterValue") String field) {
        String value = response.header(field);
        if (value == null) {
            return -1;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
