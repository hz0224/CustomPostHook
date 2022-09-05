package flume;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.util.List;
import java.util.Map;

//flume拦截器
public class MyInterceptor implements Interceptor {


    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> map = event.getHeaders();
        String value = new String(event.getBody());
        String[] data = value.split(",");
        String ts = data[0];
        String str = data[1];
        //ts要为13位毫秒数.
        map.put("timestamp", ts);

        //取出event中的日志类型，加到header中.
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    /**
     * 通过该静态内部类来创建自定义对象供flume使用，实现Interceptor.Builder接口，并实现其抽象方法
     */
    public static class Builder implements Interceptor.Builder {
        /**
         * 该方法主要用来返回创建的自定义类拦截器对象
         * @return
         */
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {
            //可以通过context得到 flume.conf中设置的参数 ，传递给Interceptor
        }

    }

}