import cc.colorcat.diskcache.DiskCache;

import java.io.*;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final String UTF8 = "UTF-8";
    private static final File HOME = new File(System.getProperty("user.home"));
    private static final File DOWNLOAD = new File(HOME, "Downloads");
    private static final String TEST_A = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String TEST_B = "01234567899876543210";
    private static final String TEST_C = "sdfgasogj 3pweotr ssdfsdfsdfd safs dafasfsaf";
    private static final Executor EXECUTOR = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            10L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    ;

    public static void main(String[] args) throws IOException {
        DiskCache diskCache = DiskCache.open(DOWNLOAD, 10 * 1024 * 1024L);
        String[] keys = {"test_a", "test_b", "test_c"};
        String[] sources = {TEST_A, TEST_B, TEST_C};
        sources[0] = readString(new FileInputStream(new File(DOWNLOAD, "test.txt")));
        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            int index = random.nextInt(1);
            int a = random.nextInt(100);
            Runnable runnable;
            if (a > 99) {
                runnable = new DeleteTask(diskCache, keys[index]);
            } else {
                if ((a & 1) == 0) {
                    runnable = new WriteTask(diskCache, keys[index], sources[index]);
                } else {
                    runnable = new ReadTask(diskCache, keys[index]);
                }
            }
            EXECUTOR.execute(runnable);
        }
    }


    private static class DeleteTask implements Runnable {
        private final DiskCache diskCache;
        private final String key;

        private DeleteTask(DiskCache cache, String key) {
            this.diskCache = cache;
            this.key = key;
        }

        @Override
        public void run() {
            try {
                diskCache.getSnapshot(key).requireDelete();
                System.out.println("delete " + key + " commit");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static class WriteTask implements Runnable {
        private final DiskCache diskCache;
        private final String key;
        private final String sources;

        private WriteTask(DiskCache cache, String key, String sources) {
            this.diskCache = cache;
            this.key = key;
            this.sources = sources;
        }

        @Override
        public void run() {
            OutputStream os = null;
            try {
                os = diskCache.getSnapshot(key).getOutputStream();
                if (os != null) {
                    writeString(sources, os);
                    System.out.println("writeString success, sources = " + sources);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                close(os);
            }
        }
    }


    private static class ReadTask implements Runnable {
        private final DiskCache diskCache;
        private final String key;

        ReadTask(DiskCache cache, String key) {
            this.diskCache = cache;
            this.key = key;
        }

        @Override
        public void run() {
            InputStream is = null;
            try {
                is = diskCache.getSnapshot(key).getInputStream();
                if (is != null) {
                    String s = readString(is);
                    System.out.println("readString success, s = " + s);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                close(is);
            }
        }
    }


    private static void writeString(String sources, OutputStream os) throws IOException {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(os, UTF8));
            bw.write(sources);
            bw.flush();
        } finally {
            close(bw);
        }
    }

    private static String readString(InputStream is) throws IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(is, UTF8));
            char[] buffer = new char[1024];
            StringBuilder sb = new StringBuilder();
            for (int length = br.read(buffer); length != -1; length = br.read(buffer)) {
                sb.append(buffer, 0, length);
            }
            return sb.toString();
        } finally {
            close(br);
        }
    }

    private static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignore) {

            }
        }
    }
}
