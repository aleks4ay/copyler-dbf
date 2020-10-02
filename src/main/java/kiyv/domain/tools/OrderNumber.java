package kiyv.domain.tools;

public final class OrderNumber {

    public static int getOrderNumberFromDocNumber(String s1) {
        String tempString = "";
        for (int i = s1.length() - 1; i >= 0; i--) {
            if (Character.isDigit(s1.charAt(i))) {
                tempString = s1.charAt(i) + tempString;
            }
            else {
                break;
            }
        }
//        System.out.println("String after parcing is: " + tempString);
        return Integer.valueOf(tempString);
    }

    public static void main(String[] args) {
        getOrderNumberFromDocNumber("743");
        getOrderNumberFromDocNumber("-000000273");
        getOrderNumberFromDocNumber("СН-0000-83");
        getOrderNumberFromDocNumber("КИ--317");
        getOrderNumberFromDocNumber("KI-811");
        getOrderNumberFromDocNumber("KI-,, 125");
        getOrderNumberFromDocNumber("KI- 170");
        getOrderNumberFromDocNumber("KI-73707");
        getOrderNumberFromDocNumber("KI-2234");
        getOrderNumberFromDocNumber("KI-0002234");
    }
}
