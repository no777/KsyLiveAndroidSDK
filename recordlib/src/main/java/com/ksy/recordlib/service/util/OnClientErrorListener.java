package com.ksy.recordlib.service.util;

/**
 * Created by hansentian on 11/4/15.
 */
public interface OnClientErrorListener {

    int ERROR_MEDIACODER_START_FAILED = 1;
    int SOURCE_AUDIO = 1;
    int SOURCE_VIDEO = 2;
    int SOURCE_VIDEO_TEMP = 3;

    void onClientError(int source, int what);


}
