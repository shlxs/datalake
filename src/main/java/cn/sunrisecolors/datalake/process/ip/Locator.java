package cn.sunrisecolors.datalake.process.ip;

import java.nio.charset.Charset;

public class Locator{
    private static final Charset Utf8 = Charset.forName("UTF-8");
    private final byte[] ipData;
    private final int textOffset;
    private final int[] index;
    private final int[] indexData;
    private final int[] textStartIndex;
    private final short[] textLengthIndex;

    Locator(byte[] data) {
        this.ipData = data;
        int offset = bigEndian(data, 0);
        this.index = new int[256];
        textOffset = offset - 256 * 256 * 4;

        for (int i = 0; i < 256; i++) {
            index[i] = littleEndian(data, 4 + i * 4 * 256);
        }

        int nidx = (textOffset - 4 - 256 * 256 * 4) / 9;
        indexData = new int[nidx];
        textStartIndex = new int[nidx];
        textLengthIndex = new short[nidx];

        for (int i = 0, off = 0; i < nidx; i++) {
            off = 4 + 256 * 256 * 4 + i * 9;
            indexData[i] = bigEndian(ipData, off);
            textStartIndex[i] = ((int) ipData[off + 6] & 0xff) << 16 | ((int) ipData[off + 5] & 0xff) << 8
                    | ((int) ipData[off + 4] & 0xff);
            textLengthIndex[i] = (short) (((int) ipData[off + 7] & 0xff) << 8 | ((int) ipData[off + 8] & 0xff));

//            String str = new String(ipData, textOffset + textStartIndex[i], textLengthIndex[i], Utf8);
//            String[] ss = str.split("\t", -1);
//            if(ss.length != 13){
//                System.out.println(str);
//            }
        }
    }

    private int bigEndian(byte[] data, int offset) {
        int a = (((int) data[offset]) & 0xff);
        int b = (((int) data[offset + 1]) & 0xff);
        int c = (((int) data[offset + 2]) & 0xff);
        int d = (((int) data[offset + 3]) & 0xff);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private int littleEndian(byte[] data, int offset) {
        int a = (((int) data[offset]) & 0xff);
        int b = (((int) data[offset + 1]) & 0xff);
        int c = (((int) data[offset + 2]) & 0xff);
        int d = (((int) data[offset + 3]) & 0xff);
        return (d << 24) | (c << 16) | (b << 8) | a;
    }

    static byte parseOctet(String ipPart) {
        // Note: we already verified that this string contains only hex digits.
        int octet = Integer.parseInt(ipPart);
        // Disallow leading zeroes, because no clear standard exists on
        // whether these should be interpreted as decimal or octal.
        if (octet < 0 || octet > 255 || (ipPart.startsWith("0") && ipPart.length() > 1)) {
            throw new NumberFormatException("invalid ip part");
        }
        return (byte) octet;
    }

    static byte[] textToNumericFormatV4(String str) {
        String[] s = str.split("\\.");
        if (s.length != 4) {
            throw new NumberFormatException("the ip is not v4");
        }
        byte[] b = new byte[4];
        b[0] = parseOctet(s[0]);
        b[1] = parseOctet(s[1]);
        b[2] = parseOctet(s[2]);
        b[3] = parseOctet(s[3]);
        return b;
    }

    public LocationInfo find(String ip) {
        byte[] b;
        try {
            b = textToNumericFormatV4(ip);
        } catch (Exception e) {
            return null;
        }
        return find(b);
    }

    private LocationInfo find(byte[] ipBin) {
        int end = indexData.length - 1;
        int a = 0xff & ((int) ipBin[0]);
        if (a != 0xff) {
            end = index[a + 1];
        }
        long ip = (long) bigEndian(ipBin, 0) & 0xffffffffL;
        int idx = findIndexOffset(ip, index[a], end);
        int off = textStartIndex[idx];
        return buildInfo(ipData, textOffset + off, 0xffff & (int) textLengthIndex[idx]);
    }

    private int findIndexOffset(long ip, int start, int end) {
        int mid = 0;
        while (start < end) {
            mid = (start + end) / 2;
            long l = 0xffffffffL & ((long) indexData[mid]);
            if (ip > l) {
                start = mid + 1;
            } else {
                end = mid;
            }
        }
        long l = ((long) indexData[end]) & 0xffffffffL;
        if (l >= ip) {
            return end;
        }
        return start;
    }

    /**
     *     "data": [
     *         "中国",                // 国家
     *         "天津",                // 省会或直辖市
     *         "天津",                // 地区或城市 , 可能不存在
     *         "",                   // 学校或单位  , 可能不存在
     *         "鹏博士",              // 运营商字段  , 可能不存在
     *         "39.128399",          // 纬度 , 可能不存在
     *         "117.185112",         // 经度 , 可能不存在
     *         "Asia/Shanghai",      // 时区一, 可能不存在
     *         "UTC+8",              // 时区二, 可能不存在
     *         "120000",             // 中国行政区划代码  (仅中国)
     *         "86",                 // 国际电话代码  , 可能不存在
     *         "CN",                 // 国家二位代码  , 可能不存在
     *         "AP"                  // 世界大洲代码  , 可能不存在
     *     ]
     * @param bytes
     * @param offset
     * @param len
     * @return
     */
    static LocationInfo buildInfo(byte[] bytes, int offset, int len) {
        String str = new String(bytes, offset, len, Utf8);
        String[] ss = str.split("\t", -1);
        if (ss.length == 13) {
            LocationInfo locationInfo = new LocationInfo();
            locationInfo.setCountry(ss[0]);       // 国家
            locationInfo.setProvince(ss[1]);      // 省会或直辖市
            locationInfo.setCity(ss[2]);          // 地区或城市 , 可能不存在
            locationInfo.setIsp(ss[4]);           // 运营商字段  , 可能不存在
            return locationInfo;
        }
        return null;
    }

}
