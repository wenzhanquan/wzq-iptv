package com.bjunicom.iptv.player;

import com.google.android.exoplayer2.C;
import com.google.android.exoplayer2.DefaultLoadControl;
import com.google.android.exoplayer2.Renderer;
import com.google.android.exoplayer2.Timeline;
import com.google.android.exoplayer2.source.MediaPeriodId;
import com.google.android.exoplayer2.source.TrackGroupArray;
import com.google.android.exoplayer2.trackselection.ExoTrackSelection;
import com.google.android.exoplayer2.upstream.Allocator;
import com.google.android.exoplayer2.upstream.DefaultAllocator;

/**
 * UTVPlayer 专用缓冲控制器 (UTVLoadControl)
 *
 * 1. 4K/超清流使用大缓冲（minBuffer 3s、maxBuffer 10s），避免高码率流频繁进入"正在缓存"；
 * 2. 高清/标清流保持低延迟起播（120ms），秒切不卡顿；
 * 3. 共享 DefaultAllocator 供后台 PreloadMediaSource 的 3 个 ProgressiveMediaPeriod 与主解码器共用。
 */
public class UTVLoadControl extends DefaultLoadControl {
    // 4K 超清大缓冲（防卡顿）
    public static final int UTV_MIN_BUFFER_MS = 3000;
    public static final int UTV_MAX_BUFFER_MS = 10000;
    public static final int UTV_BUFFER_FOR_PLAYBACK_MS = 500;
    public static final int UTV_BUFFER_FOR_PLAYBACK_AFTER_REBUFFER_MS = 800;
    public static final int UTV_TARGET_BUFFER_BYTES = 24 * 1024 * 1024;
    public static final int UTV_BACK_BUFFER_DURATION_MS = 1500;

    // 高清/标清低延迟起播（秒切）
    public static final int UTV_HD_BUFFER_FOR_PLAYBACK_MS = 120;
    public static final int UTV_HD_BUFFER_FOR_PLAYBACK_AFTER_REBUFFER_MS = 400;

    private final DefaultAllocator sharedAllocator;
    private volatile boolean is4kStream = false;

    public UTVLoadControl() {
        this(new DefaultAllocator(true, C.DEFAULT_BUFFER_SEGMENT_SIZE));
    }

    public UTVLoadControl(DefaultAllocator allocator) {
        super(
                allocator,
                UTV_MIN_BUFFER_MS,
                UTV_MAX_BUFFER_MS,
                UTV_BUFFER_FOR_PLAYBACK_MS,
                UTV_BUFFER_FOR_PLAYBACK_AFTER_REBUFFER_MS,
                UTV_TARGET_BUFFER_BYTES,
                true,
                UTV_BACK_BUFFER_DURATION_MS,
                true
        );
        this.sharedAllocator = allocator;
    }

    public void set4kStream(boolean is4k) {
        this.is4kStream = is4k;
    }

    public DefaultAllocator getSharedAllocator() {
        return sharedAllocator;
    }

    @Override
    public Allocator getAllocator() {
        return sharedAllocator;
    }

    @Override
    public boolean shouldContinueLoading(long playbackPositionUs,
                                         long bufferedDurationUs,
                                         float playbackSpeed) {
        // 组播实时流经本地代理转 HTTP 后是"无限实时流"：数据实时到达、无法缓存、不可重传。
        // 一旦暂停加载（返回 false），本地代理的 out.write 会阻塞，组播 UDP 在接收缓冲堆积直至丢包。
        // 因此对实时组播流始终持续加载，永不暂停。
        return true;
    }

    @Override
    public boolean shouldStartPlayback(Timeline timeline,
                                       MediaPeriodId mediaPeriodId,
                                       long bufferedDurationUs,
                                       float playbackSpeed,
                                       boolean rebuffering,
                                       long targetLiveOffsetUs) {
        int playbackMs = is4kStream ? UTV_BUFFER_FOR_PLAYBACK_MS : UTV_HD_BUFFER_FOR_PLAYBACK_MS;
        int afterRebufferMs = is4kStream ? UTV_BUFFER_FOR_PLAYBACK_AFTER_REBUFFER_MS : UTV_HD_BUFFER_FOR_PLAYBACK_AFTER_REBUFFER_MS;
        long minThresholdUs = rebuffering ? (afterRebufferMs * 1000L) : (playbackMs * 1000L);
        if (bufferedDurationUs >= minThresholdUs) {
            return true;
        }
        return super.shouldStartPlayback(
                timeline, mediaPeriodId, bufferedDurationUs, playbackSpeed, rebuffering, targetLiveOffsetUs);
    }
}
