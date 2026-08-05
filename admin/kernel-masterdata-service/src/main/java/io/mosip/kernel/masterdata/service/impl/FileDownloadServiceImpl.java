package io.mosip.kernel.masterdata.service.impl;

import io.mosip.kernel.masterdata.service.FileDownloadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;

@Service
public class FileDownloadServiceImpl implements FileDownloadService {

    private static final Logger logger = LoggerFactory.getLogger(FileDownloadServiceImpl.class);
    private static final String USER_AGENT = "MOSIP-MasterData-BioSDK-Downloader";

    @Value("${mosip.kernel.biosdk.file.url}")
    private String fileUrl;

    @Value("${mosip.kernel.biosdk.file.cache-path:${java.io.tmpdir}/mosip-biosdk/Bio_SDK.zip}")
    private String cachePath;

    @Value("${mosip.kernel.biosdk.file.connection-timeout:600000}")
    private int connectionTimeout;

    @Value("${mosip.kernel.biosdk.file.read-timeout:0}")
    private int readTimeout;

    @Value("${mosip.kernel.biosdk.file.max-retries:3}")
    private int maxRetries;

    @Value("${mosip.kernel.biosdk.file.retry-backoff-ms:2000}")
    private long retryBackoffMs;

    @Value("${mosip.kernel.biosdk.file.buffer-size:65536}")
    private int bufferSize;

    private final Object downloadLock = new Object();

    @Override
    public Path downloadZip() throws Exception {
        synchronized (downloadLock) {
            Path destination = Paths.get(cachePath);
            if (Files.exists(destination) && Files.size(destination) > 0) {
                logger.info("Serving Bio SDK zip from cache: {}", destination);
                return destination;
            }

            logger.info("Bio SDK zip not found in cache. Downloading from URL: {}", fileUrl);
            downloadWithRetry(destination);
            return destination;
        }
    }

    private void downloadWithRetry(Path destination) throws Exception {
        Exception lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                long backoff = calculateBackoff(attempt);
                logger.warn("Retrying Bio SDK download. attempt={}/{} backoffMs={}", attempt, maxRetries, backoff);
                Thread.sleep(backoff);
            }

            try {
                performDownload(destination);
                return;
            } catch (NonRetryableDownloadException e) {
                cleanupPartialFile(destination);
                throw e;
            } catch (Exception e) {
                lastException = e;
                logger.warn("Bio SDK download attempt {} failed: {}", attempt + 1, e.getMessage());
            }
        }

        cleanupPartialFile(destination);
        throw new DownloadException("Bio SDK download failed after retries", lastException);
    }

    private long calculateBackoff(int attempt) {
        long exponentialBackoff = retryBackoffMs * (1L << Math.min(attempt - 1, 5));
        return Math.min(exponentialBackoff, 60000L);
    }

    private void performDownload(Path destination) throws Exception {
        ensureParentDirectory(destination);
        Path partialPath = getPartialPath(destination);
        long resumeOffset = Files.exists(partialPath) ? Files.size(partialPath) : 0;
        HttpURLConnection connection = null;
        Instant startedAt = Instant.now();

        try {
            connection = openConnection(resumeOffset);
            int responseCode = connection.getResponseCode();
            long effectiveOffset = validateResponse(responseCode, resumeOffset, partialPath);
            long totalSize = resolveTotalSize(connection, effectiveOffset);

            logger.info("Bio SDK download started. responseCode={} resumeOffset={} expectedBytes={}",
                    responseCode, effectiveOffset, totalSize);

            try (InputStream inputStream = new BufferedInputStream(connection.getInputStream(), bufferSize);
                 OutputStream outputStream = openOutputStream(partialPath, effectiveOffset)) {
                byte[] buffer = new byte[bufferSize];
                long downloaded = effectiveOffset;
                long lastLoggedAt = System.currentTimeMillis();
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                    downloaded += bytesRead;
                    lastLoggedAt = logProgress(downloaded, totalSize, startedAt, lastLoggedAt);
                }

                outputStream.flush();
                verifyDownloadedSize(downloaded, totalSize);
            }

            movePartialFile(partialPath, destination);
            logger.info("Bio SDK download completed successfully: {}", destination);
        } catch (FileNotFoundException e) {
            throw new NonRetryableDownloadException("Bio SDK file not found at " + fileUrl, e);
        } catch (MalformedURLException | URISyntaxException e) {
            throw new NonRetryableDownloadException("Invalid Bio SDK URL: " + fileUrl, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private HttpURLConnection openConnection(long resumeOffset) throws IOException, URISyntaxException {
        URL url = new URI(fileUrl).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(connectionTimeout);
        connection.setReadTimeout(readTimeout);
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept-Encoding", "identity");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setInstanceFollowRedirects(true);

        if (resumeOffset > 0) {
            connection.setRequestProperty("Range", "bytes=" + resumeOffset + "-");
            logger.info("Resuming Bio SDK download from byte {}", resumeOffset);
        }

        connection.connect();
        return connection;
    }

    private long validateResponse(int responseCode, long resumeOffset, Path partialPath) throws Exception {
        if (responseCode == HttpURLConnection.HTTP_PARTIAL) {
            return resumeOffset;
        }

        if (responseCode == HttpURLConnection.HTTP_OK) {
            if (resumeOffset > 0) {
                logger.warn("Remote server ignored Range header. Restarting Bio SDK download from byte 0");
                Files.deleteIfExists(partialPath);
            }
            return 0;
        }

        if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED || responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new NonRetryableDownloadException("Access denied while downloading Bio SDK. HTTP " + responseCode);
        }

        if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new NonRetryableDownloadException("Bio SDK URL not found. HTTP 404");
        }

        if (responseCode == 416) {
            throw new NonRetryableDownloadException("Resume range is not satisfiable. Delete partial file and retry.");
        }

        if (responseCode >= 500) {
            throw new DownloadException("Remote server error while downloading Bio SDK. HTTP " + responseCode);
        }

        throw new NonRetryableDownloadException("Unexpected response while downloading Bio SDK. HTTP " + responseCode);
    }

    private long resolveTotalSize(HttpURLConnection connection, long resumeOffset) {
        String contentRange = connection.getHeaderField("Content-Range");
        if (contentRange != null && contentRange.contains("/")) {
            try {
                return Long.parseLong(contentRange.substring(contentRange.lastIndexOf('/') + 1));
            } catch (NumberFormatException e) {
                logger.warn("Unable to parse Content-Range header: {}", contentRange);
            }
        }

        long contentLength = connection.getContentLengthLong();
        return contentLength > 0 ? contentLength + resumeOffset : -1;
    }

    private OutputStream openOutputStream(Path partialPath, long resumeOffset) throws IOException {
        if (resumeOffset > 0 && Files.exists(partialPath)) {
            return Files.newOutputStream(partialPath, StandardOpenOption.APPEND);
        }

        return Files.newOutputStream(partialPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private long logProgress(long downloaded, long totalSize, Instant startedAt, long lastLoggedAt) {
        long now = System.currentTimeMillis();
        if (now - lastLoggedAt < 5000) {
            return lastLoggedAt;
        }

        Duration elapsed = Duration.between(startedAt, Instant.now());
        double elapsedSeconds = Math.max(elapsed.toMillis() / 1000.0, 1.0);
        double mbPerSecond = downloaded / elapsedSeconds / (1024.0 * 1024.0);

        if (totalSize > 0) {
            double percent = downloaded * 100.0 / totalSize;
            logger.info("Bio SDK download progress: {} / {} bytes, {}%, {} MB/s",
                    downloaded, totalSize, String.format("%.2f", percent), String.format("%.2f", mbPerSecond));
        } else {
            logger.info("Bio SDK download progress: {} bytes, {} MB/s",
                    downloaded, String.format("%.2f", mbPerSecond));
        }
        return now;
    }

    private void verifyDownloadedSize(long downloaded, long totalSize) throws DownloadException {
        if (totalSize > 0 && downloaded != totalSize) {
            throw new DownloadException("Bio SDK size mismatch. expected=" + totalSize + " actual=" + downloaded);
        }
    }

    private void movePartialFile(Path partialPath, Path destination) throws DownloadException {
        try {
            Files.move(partialPath, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            try {
                Files.move(partialPath, destination, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException moveException) {
                throw new DownloadException("Failed to move completed Bio SDK zip into cache", moveException);
            }
        } catch (IOException e) {
            throw new DownloadException("Failed to move completed Bio SDK zip into cache", e);
        }
    }

    private void ensureParentDirectory(Path destination) throws DownloadException {
        Path parent = destination.getParent();
        if (parent == null || Files.exists(parent)) {
            return;
        }

        try {
            Files.createDirectories(parent);
        } catch (IOException e) {
            throw new DownloadException("Failed to create Bio SDK cache directory: " + parent, e);
        }
    }

    private Path getPartialPath(Path destination) {
        return destination.resolveSibling(destination.getFileName().toString() + ".part");
    }

    private void cleanupPartialFile(Path destination) {
        try {
            Files.deleteIfExists(getPartialPath(destination));
        } catch (IOException e) {
            logger.warn("Failed to delete partial Bio SDK file", e);
        }
    }

    private static class DownloadException extends Exception {
        private static final long serialVersionUID = 1L;

        DownloadException(String message) {
            super(message);
        }

        DownloadException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static class NonRetryableDownloadException extends DownloadException {
        private static final long serialVersionUID = 1L;

        NonRetryableDownloadException(String message) {
            super(message);
        }

        NonRetryableDownloadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}