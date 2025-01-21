package vn.ghtk.connect.replicator.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

public class DateTimeUtils {

    private static DateTimeFormatter formatNormal = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter formatZulu = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static ZoneId ZONE_SYSTEM = ZoneId.systemDefault();

    public static Date convertZuluTime2Date(String zuluTime) {
        try {
            return Date.from(LocalDateTime.parse(zuluTime, formatZulu).toInstant(ZoneOffset.UTC));
        } catch (Exception e) {
            return null;
        }
    }

    public static String convertTimestamp(Date date) {
        if (Objects.isNull(date)) {
            return null;
        }
        return formatNormal.format(date.toInstant().atZone(ZONE_SYSTEM).toLocalDateTime());
    }

}
