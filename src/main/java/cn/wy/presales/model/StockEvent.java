package cn.wy.presales.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StockEvent extends Event {



    public  String tid;
    public  String tno;
    public  String tdate;
    public  String uno;
    public  String pno;
    public  Integer tnum;
    public  String tuptime ;
    //public final String event_time;

    public final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss.SSS");
    //{tid":"A476","tno":"Ak2620220126061331","tdate":"2022-01-25","uno":"U1956","pno":"P1038","tnum":3,"tuptime":"2022-01-26T06:13:31Z"}
    public StockEvent() {
        tid = "";
        tno = "";
        tdate="";
        uno = "";
        pno = "";
        tnum = 0;
        tuptime ="" ;

     //   event_time = "";
    }


    @Override
    public String toString() {
        return "StockEvent{" +
                "pno=" + pno +
         //       ", event_time=" + event_time +
                ", tnum=" + tnum +
                '}';
    }

    @Override
    public long getTimestamp() {
        Date dateTime;
        try {
            dateTime = formatter.parse(this.tuptime);
            return dateTime.getTime();

        } catch (ParseException e) {
            return 0;

        }
    }
}
