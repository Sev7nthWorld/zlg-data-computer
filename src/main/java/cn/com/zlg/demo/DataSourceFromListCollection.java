package cn.com.zlg.demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/*
Data Sources 是什么呢？就字面意思其实就可以知道：数据来源。
Flink 做为一款流式计算框架，它可用来做批处理，即处理静态的数据集、历史的数据集；也可以用来做流处理，即实时的处理些实时数据流，实时的产生数据流结果，只要数据源源不断的过来，Flink 就能够一直计算下去，这个 Data Sources 就是数据的来源地。
Flink 中你可以使用 StreamExecutionEnvironment.addSource(sourceFunction) 来为你的程序添加数据来源。
Flink 已经提供了若干实现好了的 source functions，当然你也可以通过实现 SourceFunction 来自定义非并行的 source 或者实现 ParallelSourceFunction 接口或者扩展 RichParallelSourceFunction 来自定义并行的 source
总的来说可以分为下面几大类：
基于集合
1、fromCollection(Collection) - 从 Java 的 Java.util.Collection 创建数据流。集合中的所有元素类型必须相同。
2、fromCollection(Iterator, Class) - 从一个迭代器中创建数据流。Class 指定了该迭代器返回元素的类型。
3、fromElements(T …) - 从给定的对象序列中创建数据流。所有对象类型必须相同。
4、fromParallelCollection(SplittableIterator, Class) - 从一个迭代器中创建并行数据流。Class 指定了该迭代器返回元素的类型。
5、generateSequence(from, to) - 创建一个生成指定区间范围内的数字序列的并行数据流。

基于文件
1、readTextFile(path) - 读取文本文件，即符合 TextInputFormat 规范的文件，并将其作为字符串返回。
2、readFile(fileInputFormat, path) - 根据指定的文件输入格式读取文件（一次）。
3、readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo) - 这是上面两个方法内部调用的方法。它根据给定的 fileInputFormat 和读取路径读取文件。根据提供的 watchType，这个 source 可以定期（每隔 interval 毫秒）监测给定路径的新数据（FileProcessingMode.PROCESS_CONTINUOUSLY），或者处理一次路径对应文件的数据并退出（FileProcessingMode.PROCESS_ONCE）。你可以通过 pathFilter 进一步排除掉需要处理的文件。
 */
public class DataSourceFromListCollection {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1、fromCollection(Collection) - 从 Java 的 Java.util.Collection 创建数据流。集合中的所有元素类型必须相同。
        List<String> outs1 = new ArrayList<String>();
        outs1.add("hello");
        outs1.add("world");
        DataStreamSource<String> stream1 = env.fromCollection(outs1);
        stream1.print();
        env.execute();

    }

}


/*
1、可以直接启动本程序，在cmd中输入nc -l 9000，然后输入报文，比如"hello world"，在控制台上就可以看到（world，1），（hello，1）
2、程序完成后打包，进入 flink 安装目录 bin 下执行以下命令跑程序
flink run -c com.seven.flink.study00。FirstDemo /Users/zhaoxin/idea-workspace/bigdata/flink-study/target/flink-study-1.0.0.jar 127.0.0.1 9000
执行完上述命令后，我们可以在 Flink的webUI 的Running Jobs中看到正在运行的程序。
 */