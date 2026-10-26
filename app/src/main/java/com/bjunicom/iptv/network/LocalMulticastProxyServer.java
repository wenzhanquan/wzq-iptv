package com.bjunicom.iptv.network;

import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Android 端内置轻量级 udpxy (UDP/RTP 组播转 HTTP 单播) 本地代理网关
 *
 * 作用：
 * 当路由器开启了 IGMP Proxy / Snooping 或光猫直连电视盒子，但路由器未运行 udpxy 时，
 * 本服务在 Android 本地监听 http://127.0.0.1:<port>/rtp/239.3.1.x:port，
 * 自动加入对应的 IGMP 组播组，剥离 12 字节 RTP 协议头（含 CSRC 和扩展头），
 * 将纯净的 MPEG-TS (0x47 同步字节) 视频流以 HTTP 方式实时推送给 ijkplayer 播放。
 */
public class LocalMulticastProxyServer {
    private static final String TAG = "LocalMulticastProxy";
    private static final Pattern PATH_PATTERN = Pattern.compile("/(rtp|udp)/([0-9.]+):(\\d+)");
    private static final int PROXY_PORT = 8899;  // 固定本地组播代理端口 (127.0.0.1)
    private static volatile String userPreferredInterfaceName = null;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final List<ActiveSession> activeSessions = new CopyOnWriteArrayList<>();
    private ServerSocket serverSocket;
    private ExecutorService executorService;
    private int listenPort = -1;

    private static final class ActiveSession {
        final String groupKey;
        final Socket client;
        final MulticastSocket multicastSocket;

        ActiveSession(String groupKey, Socket client, MulticastSocket multicastSocket) {
            this.groupKey = groupKey;
            this.client = client;
            this.multicastSocket = multicastSocket;
        }

        void closeQuietly() {
            try {
                multicastSocket.close();
            } catch (Exception ignored) {
            }
            try {
                client.close();
            } catch (Exception ignored) {
            }
        }
    }

    public synchronized int start() {
        if (running.get() && listenPort > 0) {
            return listenPort;
        }
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            try {
                serverSocket.bind(new InetSocketAddress("127.0.0.1", PROXY_PORT));
                listenPort = PROXY_PORT;
            } catch (IOException bindEx) {
                serverSocket.bind(new InetSocketAddress("127.0.0.1", 0));
                listenPort = serverSocket.getLocalPort();
                Log.w(TAG, "Fixed proxy port " + PROXY_PORT + " occupied, fallback to random port " + listenPort);
            }
            running.set(true);
            executorService = Executors.newCachedThreadPool();
            executorService.execute(this::acceptLoop);
            Log.i(TAG, "Local udpxy proxy started on 127.0.0.1:" + listenPort);
            return listenPort;
        } catch (IOException e) {
            Log.e(TAG, "Failed to start local multicast proxy", e);
            return -1;
        }
    }

    public int getListenPort() {
        return listenPort;
    }

    public synchronized void stop() {
        running.set(false);
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
            serverSocket = null;
        }
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        listenPort = -1;
    }

    private void acceptLoop() {
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                if (executorService != null && !executorService.isShutdown()) {
                    executorService.execute(() -> handleClient(client));
                } else {
                    client.close();
                }
            } catch (IOException e) {
                if (running.get()) {
                    Log.w(TAG, "Accept error: " + e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket client) {
        // 组播实时流转发对调度延迟敏感，提高本转发线程优先级，减少 GC/UI 抢占导致的丢包
        try {
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        } catch (Throwable ignored) {
        }
        MulticastSocket multicastSocket = null;
        InetAddress groupAddr = null;
        int mcastPort = -1;
        NetworkInterface mcastIf = null;
        boolean joinedWithInterface = false;
        ActiveSession currentSession = null;
        try {
            client.setSoTimeout(8000);
            client.setTcpNoDelay(true);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(client.getInputStream(), StandardCharsets.US_ASCII));
            String requestLine = reader.readLine();
            if (requestLine == null || requestLine.isEmpty()) {
                return;
            }
            // 读取并丢弃剩余 HTTP 请求头
            String headerLine;
            while ((headerLine = reader.readLine()) != null && !headerLine.isEmpty()) {
                // consume headers
            }

            String[] parts = requestLine.split(" ");
            if (parts.length < 2) {
                return;
            }
            String path = parts[1];
            Matcher matcher = PATH_PATTERN.matcher(path);
            if (!matcher.find()) {
                writeHttpError(client.getOutputStream(), 400, "Bad Request");
                return;
            }

            String proto = matcher.group(1);
            String mcastIp = matcher.group(2);
            mcastPort = Integer.parseInt(matcher.group(3));
            String groupKey = mcastIp + ":" + mcastPort;

            // 切台时立即关闭其它频道残留的组播连接，避免同端口(如8000/8008)旧频道串流或占用带宽
            for (ActiveSession session : activeSessions) {
                if (!groupKey.equals(session.groupKey)) {
                    activeSessions.remove(session);
                    session.closeQuietly();
                }
            }

            groupAddr = InetAddress.getByName(mcastIp);
            multicastSocket = new MulticastSocket(null);
            multicastSocket.setReuseAddress(true);
            try {
                // 优先绑定具体组播 IP+端口，防止 Linux 内核将同端口不同组播组的包混入当前 Socket
                multicastSocket.bind(new InetSocketAddress(groupAddr, mcastPort));
            } catch (Exception bindEx) {
                multicastSocket.bind(new InetSocketAddress(mcastPort));
            }
            multicastSocket.setReceiveBufferSize(4 * 1024 * 1024);
            multicastSocket.setSoTimeout(5000);
            mcastIf = findPreferredMulticastInterface();
            if (mcastIf != null) {
                try {
                    multicastSocket.setNetworkInterface(mcastIf);
                } catch (Exception ignored) {
                }
                try {
                    multicastSocket.joinGroup(new InetSocketAddress(groupAddr, mcastPort), mcastIf);
                    joinedWithInterface = true;
                } catch (Exception ignored) {
                }
            }
            if (!joinedWithInterface) {
                multicastSocket.joinGroup(groupAddr);
            }

            currentSession = new ActiveSession(groupKey, client, multicastSocket);
            activeSessions.add(currentSession);

            OutputStream out = client.getOutputStream();
            String responseHeaders = "HTTP/1.1 200 OK\r\n"
                    + "Content-Type: video/mp2t\r\n"
                    + "Cache-Control: no-cache\r\n"
                    + "Connection: close\r\n"
                    + "Server: BjUnicomIptv-LocalUdpxy/1.0\r\n\r\n";
            out.write(responseHeaders.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            // 8192 字节足够容纳 jumbo 巨帧组播包（部分光猫用大 UDP 包提升效率），
            // 过小的 buffer 会被内核静默截断，导致 TS 流损坏出现马赛克
            byte[] buffer = new byte[8192];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            boolean isRtp = "rtp".equalsIgnoreCase(proto);

            // 接收线程与写线程分离：组播实时流若"边收边写"同步串行，
            // 一旦播放器读得慢（out.write 阻塞），接收即停，内核接收缓冲堆满就丢包。
            // 用有界队列解耦后，接收线程持续 receive 不阻塞，从根本上减少丢包。
            final BlockingQueue<byte[]> pendingTs = new ArrayBlockingQueue<>(1024);
            final AtomicBoolean receiverDone = new AtomicBoolean(false);
            final MulticastSocket recvSocket = multicastSocket;
            final byte[] recvBuf = buffer;
            final DatagramPacket recvPacket = packet;
            final boolean recvIsRtp = isRtp;

            Thread receiver = new Thread(() -> {
                try {
                    while (running.get() && !client.isClosed()) {
                        recvSocket.receive(recvPacket);
                        int len = recvPacket.getLength();
                        int offset = recvPacket.getOffset();
                        if (len <= 0) {
                            continue;
                        }

                        // 自动剥离 RTP 协议头，输出 MPEG-TS 188 字节对齐负载
                        int payloadOffset = offset;
                        int payloadLen = len;
                        if (recvIsRtp && len > 12 && (recvBuf[offset] & 0xFF) != 0x47) {
                            int version = (recvBuf[offset] & 0xC0) >>> 6;
                            if (version == 2) {
                                int csrcCount = recvBuf[offset] & 0x0F;
                                boolean hasExtension = (recvBuf[offset] & 0x10) != 0;
                                boolean hasPadding = (recvBuf[offset] & 0x20) != 0;
                                int headerSize = 12 + csrcCount * 4;
                                if (hasExtension && len >= headerSize + 4) {
                                    int extLenWords = ((recvBuf[offset + headerSize + 2] & 0xFF) << 8)
                                            | (recvBuf[offset + headerSize + 3] & 0xFF);
                                    headerSize += 4 + extLenWords * 4;
                                }
                                int paddingBytes = 0;
                                if (hasPadding) {
                                    paddingBytes = recvBuf[offset + len - 1] & 0xFF;
                                }
                                if (headerSize + paddingBytes < len) {
                                    payloadOffset = offset + headerSize;
                                    payloadLen = len - headerSize - paddingBytes;
                                }
                            }
                        }

                        byte[] ts = Arrays.copyOfRange(recvBuf, payloadOffset, payloadOffset + payloadLen);
                        // 队列满则丢最旧包：组播实时流丢旧比阻塞（进而丢新包）更优
                        if (!pendingTs.offer(ts)) {
                            pendingTs.poll();
                            pendingTs.offer(ts);
                        }
                    }
                } catch (Exception ignored) {
                } finally {
                    receiverDone.set(true);
                }
            }, "LocalUdpxy-Rx");
            receiver.setPriority(Thread.MAX_PRIORITY);
            receiver.setDaemon(true);
            receiver.start();

            // 写线程（当前线程）：从队列取 TS 数据写回 HTTP 客户端
            while (running.get() && !client.isClosed()) {
                byte[] ts = pendingTs.poll(2000, TimeUnit.MILLISECONDS);
                if (ts != null) {
                    out.write(ts, 0, ts.length);
                } else if (receiverDone.get() && pendingTs.isEmpty()) {
                    break;
                }
            }
        } catch (Exception e) {
            Log.d(TAG, "Stream closed: " + e.getMessage());
        } finally {
            if (currentSession != null) {
                activeSessions.remove(currentSession);
            }
            if (multicastSocket != null) {
                if (groupAddr != null) {
                    try {
                        if (joinedWithInterface && mcastIf != null && mcastPort > 0) {
                            multicastSocket.leaveGroup(new InetSocketAddress(groupAddr, mcastPort), mcastIf);
                        } else {
                            multicastSocket.leaveGroup(groupAddr);
                        }
                    } catch (Exception ignored) {
                    }
                }
                try {
                    multicastSocket.close();
                } catch (Exception ignored) {
                }
            }
            try {
                client.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void writeHttpError(OutputStream out, int code, String message) throws IOException {
        String resp = "HTTP/1.1 " + code + " " + message + "\r\nConnection: close\r\n\r\n";
        out.write(resp.getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    /**
     * 优先查找 eth0 / usb0 / wlan0 等已启用且具备 IPv4 地址的组播网卡（支持有线网卡直连光猫 IPTV 口）
     */
    public static void setPreferredInterfaceName(String name) {
        userPreferredInterfaceName = (name == null || name.trim().isEmpty()) ? null : name.trim();
    }

    public static String getPreferredInterfaceName() {
        return userPreferredInterfaceName;
    }

    public static List<NetworkInterface> listAvailableInterfaces() {
        List<NetworkInterface> result = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                for (NetworkInterface nif : Collections.list(interfaces)) {
                    if (!nif.isLoopback() && hasIpv4Address(nif)) {
                        result.add(nif);
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return result;
    }

    public static NetworkInterface findPreferredMulticastInterface() {
        try {
            // 1. 用户指定网卡优先
            if (userPreferredInterfaceName != null && !userPreferredInterfaceName.isEmpty()) {
                NetworkInterface nif = NetworkInterface.getByName(userPreferredInterfaceName);
                if (nif != null && nif.isUp() && !nif.isLoopback() && hasIpv4Address(nif)) {
                    return nif;
                }
            }
            // 2. 默认硬编码顺序（有线优先）
            String[] preferredNames = {"eth0", "eth1", "usb0", "rndis0", "wlan0"};
            for (String name : preferredNames) {
                NetworkInterface nif = NetworkInterface.getByName(name);
                if (nif != null && nif.isUp() && !nif.isLoopback() && hasIpv4Address(nif)) {
                    return nif;
                }
            }
            // 3. 兜底：遍历所有网卡
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                for (NetworkInterface nif : Collections.list(interfaces)) {
                    if (nif.isUp() && !nif.isLoopback() && nif.supportsMulticast() && hasIpv4Address(nif)) {
                        return nif;
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private static boolean hasIpv4Address(NetworkInterface nif) {
        Enumeration<InetAddress> addrs = nif.getInetAddresses();
        while (addrs.hasMoreElements()) {
            if (addrs.nextElement() instanceof Inet4Address) {
                return true;
            }
        }
        return false;
    }

    public static void bindMulticastSocketWithGroupFilter(
            MulticastSocket socket, InetAddress groupAddr, int mcastPort) throws IOException {
        if (groupAddr != null && groupAddr.isMulticastAddress()) {
            try {
                socket.bind(new InetSocketAddress(groupAddr, mcastPort));
                return;
            } catch (IOException ignored) {
            }
        }
        socket.bind(new InetSocketAddress(mcastPort));
    }

    public static List<NetworkInterface> getCandidateMulticastInterfaces() {
        List<NetworkInterface> list = new ArrayList<>();
        try {
            String[] preferredNames = {"eth0.3964", "eth0", "eth1", "usb0", "rndis0", "wlan0"};
            for (String name : preferredNames) {
                NetworkInterface nif = NetworkInterface.getByName(name);
                if (nif != null && nif.isUp() && !nif.isLoopback() && nif.supportsMulticast() && hasIpv4Address(nif)) {
                    list.add(nif);
                }
            }
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                for (NetworkInterface nif : Collections.list(interfaces)) {
                    if (nif.isUp() && !nif.isLoopback() && nif.supportsMulticast() && hasIpv4Address(nif)) {
                        boolean exists = false;
                        for (NetworkInterface existing : list) {
                            if (existing.getName().equals(nif.getName())) {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) {
                            list.add(nif);
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return list;
    }

    public static List<NetworkInterface> joinMulticastOnAllInterfaces(
            MulticastSocket socket, InetAddress groupAddr, int mcastPort) {
        List<NetworkInterface> joinedInterfaces = new ArrayList<>();
        List<NetworkInterface> candidates = getCandidateMulticastInterfaces();
        if (!candidates.isEmpty()) {
            for (NetworkInterface nif : candidates) {
                try {
                    if (joinedInterfaces.isEmpty()) {
                        socket.setNetworkInterface(nif);
                    }
                    socket.joinGroup(new InetSocketAddress(groupAddr, mcastPort), nif);
                    joinedInterfaces.add(nif);
                    String name = nif.getName();
                    if (name != null && (name.startsWith("eth") || name.startsWith("usb") || name.startsWith("rndis"))) {
                        break;
                    }
                } catch (Exception ignored) {
                }
            }
        }
        if (joinedInterfaces.isEmpty()) {
            try {
                socket.joinGroup(groupAddr);
            } catch (Exception ignored) {
            }
        }
        return joinedInterfaces;
    }

    public static void leaveMulticastOnAllInterfaces(
            MulticastSocket socket, InetAddress groupAddr, int mcastPort, List<NetworkInterface> joinedInterfaces) {
        if (socket == null || groupAddr == null) {
            return;
        }
        if (joinedInterfaces != null && !joinedInterfaces.isEmpty()) {
            for (NetworkInterface nif : joinedInterfaces) {
                try {
                    socket.leaveGroup(new InetSocketAddress(groupAddr, mcastPort), nif);
                } catch (Exception ignored) {
                }
            }
        } else {
            try {
                socket.leaveGroup(groupAddr);
            } catch (Exception ignored) {
            }
        }
    }
}
