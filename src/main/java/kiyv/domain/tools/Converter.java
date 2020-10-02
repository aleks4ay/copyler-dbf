package kiyv.domain.tools;

public final class Converter {
    private static String oldSequense = "\r\n";
    private static String oldSequenseTwo = "\n";
    private static String newSequense = "uUu";


    public static String deleteEnter(String s1) {
        if (s1.contains("\n") | s1.contains("\r\n")) {
            s1 = s1.replace(oldSequense, newSequense);
            s1 = s1.replace(oldSequenseTwo, newSequense);
        }
        return s1;
    }

    public static String deleteEnterForever(String s1) {
        if (s1.contains("\n") | s1.contains("\r\n")) {
            s1 = s1.replace(oldSequense, " ");
            s1 = s1.replace(oldSequenseTwo, " ");
        }
        return s1;
    }

    public static String returnEnter(String s1) {
        if (s1.contains(newSequense)) {
            s1 = s1.replace(newSequense, oldSequenseTwo);
        }
        return s1;
    }

    public static int convertStrToInt(String s1) {
        int result = 0;
        int x5 = (int) s1.charAt(4);
        int x6 = (int) s1.charAt(5);
        if (!s1.substring(4,5).equals(" ")) {
            if (x5 > 64) {
                result += 36 * (x5 - 55);
            }
            else {
                result += 36 * Integer.valueOf(s1.substring(4,5));
            }
        }
        if (!s1.substring(5,6).equals(" ")) {
            if (x6 > 64) {
                result += x6 - 55;
            }
            else {
                result += Integer.valueOf(s1.substring(5,6));
            }
        }
        return result;
    }
}
