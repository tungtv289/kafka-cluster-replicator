package vn.ghtk.connect.replicator.utils;

import java.util.Objects;

public class StringBaseUtils {

    public static boolean equals(String str1, String str2) {
        if (Objects.isNull(str1) && Objects.isNull(str2)) {
            return true;
        }
        if (Objects.isNull(str1) || Objects.isNull(str2)) {
            return false;
        }
        return str1.equals(str2);
    }
}
