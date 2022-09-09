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

//自定义一个 UDTF 实现将一个任意分割符的字符串切割成独立的单词
public class MyUDTF extends GenericUDTF {

    //初始化方法，这里做一些列名的定义
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //1.定义输出数据的默认列名，可以被定义的别名覆盖.
        List<String> fieldNames = new ArrayList<>(); //因为可能会炸出多个列，因此用一个list来存
        List<ObjectInspector> fieldOIs = new ArrayList<>();//多个列自然对应多个数据类型，也用list存.

        //2.添加输出数据的列名和类型
        fieldNames.add("word");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //返回列名和列类型.
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        //System.out.println(args[0].getClass()); // org.apache.hadoop.io.Text 传进来的参数的类型并不是String，而是Text.

        //1 获取第一个参数值，数据
        String value =  args[0].toString();

        //2 获取第二个参数值，分隔符
        String splitKey = args[1].toString();

        //3 切割数据
        String[] data = value.split(splitKey);

        //4 遍历数据写出
        for (String field : data) {
            //list用来封装结果，由于结果是单词 String类型，因此这里使用String
            ArrayList<String> outList = new ArrayList<>();
            outList.add(field);
            //UDTF要求写出时必须使用集合封装结果，集合里有一个元素就是炸出一列，有两个元素就是炸出两列，以此类推。
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }


}
