package io.mosip.kernel.masterdata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.mosip.kernel.masterdata.service.FileDownloadService;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/download")
public class DownloadBioSdkZipController {

    private static final Logger logger = LoggerFactory.getLogger(DownloadBioSdkZipController.class);
    private static final int BUFFER_SIZE = 8192; // 8KB buffer is standard and safe

    @Autowired
    private FileDownloadService fileDownloadService;

    @GetMapping("/bio-sdk")
    public void downloadFile(HttpServletResponse response) throws Exception {
        logger.info("Received request to download Bio SDK zip file");

        // 1. Get the file path (either downloads from GitHub or returns cached path)
        Path bioSdkZip = fileDownloadService.downloadZip();

        if (!Files.exists(bioSdkZip)) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Bio SDK file not found");
            return;
        }

        // 2. Set headers
        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=Bio_SDK.zip");
        response.setContentLengthLong(Files.size(bioSdkZip));

        // 3. Stream the file synchronously
        try (InputStream inputStream = Files.newInputStream(bioSdkZip);
             OutputStream out = response.getOutputStream()) {

            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
            logger.info("File successfully streamed to client");
        } catch (Exception e) {
            logger.error("Error during file streaming: {}", e.getMessage());
        }
    }
}