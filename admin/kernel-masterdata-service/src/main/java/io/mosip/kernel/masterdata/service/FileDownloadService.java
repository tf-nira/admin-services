package io.mosip.kernel.masterdata.service;

import java.nio.file.Path;

public interface FileDownloadService {

    Path downloadZip() throws Exception;
}