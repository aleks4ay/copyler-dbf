import kiyv.domain.tools.DateConverter;

/**
 * Created by aser on 28.11.2019.
 */
public class TimeFrom1CTest {
    public static void main(String[] args) {
        int bigNumber = (DateConverter.getYearShort(DateConverter.getNowDate()) ) * 100000 + 2;
        System.out.println(bigNumber);
//        DataControl.writeTimeChange();
//        DataControl.writeTimeChangeFrom1C();
    }
}
