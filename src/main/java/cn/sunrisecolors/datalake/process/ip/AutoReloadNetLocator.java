package cn.sunrisecolors.datalake.process.ip;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by long on 2017/1/17.
 */
public class AutoReloadNetLocator extends TimerTask {

    private final String fileUrl;
    private Locator locator;
    private String lastModified;

    public AutoReloadNetLocator(String fileUrl, int intervalSeconds) throws Exception {
        this.fileUrl = fileUrl;
        locator = loadFromNet(fileUrl);
        Timer timer = new Timer();
        timer.schedule(this, 10000 , intervalSeconds * 1000);
    }

    @Override
    public void run() {
        try {
            String netFileModifyTime = getNetFileModifyTime(this.fileUrl);
            if (lastModified != null && !lastModified.equals(netFileModifyTime)) {
                lastModified = netFileModifyTime;
                locator = loadFromNet(fileUrl);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private String getNetFileModifyTime(String fileUrl) throws Exception {
        URL url = new URL(fileUrl);
        HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("HEAD");
        int responseCode = httpURLConnection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            List<String> lastModified = httpURLConnection.getHeaderFields().get("Last-Modified");
            if (lastModified.size() > 0) {
                return lastModified.get(0);
            }
        }
        return null;
    }

    private Locator loadFromNet(String fileUrl) throws IOException {
        URL url = new URL(fileUrl);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setConnectTimeout(3000);
        httpConn.setReadTimeout(30 * 1000);
        int responseCode = httpConn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("read error, response code is " + responseCode);
        }
        int length = httpConn.getContentLength();
        if (length <= 0 || length > 64 * 1024 * 1024) {
            throw new InputMismatchException("invalid ip data");
        }
        InputStream is = httpConn.getInputStream();
        byte[] data = new byte[length];
        int downloaded = 0;
        int read = 0;
        while (downloaded < length) {
            try {
                read = is.read(data, downloaded, length - downloaded);
            } catch (IOException e) {
                is.close();
                throw new IOException("read error");
            }
            if (read < 0) {
                is.close();
                throw new IOException("read error");
            }
            downloaded += read;
        }

        is.close();

        return new Locator(data);
    }

    public LocationInfo find(String ip) {
        return locator.find(ip);
    }

}
