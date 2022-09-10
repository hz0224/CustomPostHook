package hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 输入数据：
 *      hello,java:hello,hive
 * 输出数据：
 *      hello   java
 *      hello   hive
 */
public class CustomUDTF extends GenericUDTF {

    //输出数据的集合，为了实现复用，这里定义成全局变量
    private List<String> outList =  new ArrayList<String>();

    @Override//初始化方法，定义默认数据类型和列名
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1.定义输出数据的默认列名，可以被定义的别名覆盖.
        List<String> fieldNames = new ArrayList<>(); //因为可能会炸出多个列，因此用一个list来存
        List<ObjectInspector> fieldOIs = new ArrayList<>();//多个列自然对应多个数据类型，也用list存.

        //2.添加输出数据的列名和类型
        fieldNames.add("word1");
        fieldNames.add("word2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //返回列名和列类型.
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String value = args[0].toString();
        String[] fields = value.split(":");

        for (String field : fields) {
            //每次输出前先清空集合
            outList.clear();

            String[] words = field.split(",");
            outList.add(words[0]); //第一列
            outList.add(words[1]); //第二列

            //写出
            forward(outList); //每次写出时，outList集合有2个元素，因此就是炸出2列.
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
