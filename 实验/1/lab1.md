# 实验三 文档倒排索引算法

**151220129 计科 吴政亿 nju_wzy@163.com**

**151220130 计科 伍昱名 707512433@qq.com  **

**151220135 计科 许丽军**

**151220142 计科 杨楠  1158864287@qq.com  **

## 1 实验目的

1. 应用课堂上介绍的“带词频属性的文档倒排算法”

2. 在统计词语的倒排索引时，除了要输出带词频属性的倒排索引，还请计算每个词语的“平均提及次数”并输出。

   > 平均提及次数= 词语在全部文档中出现的频数总和/ 包含该词语的文档数

3. 两个计算任务请在同一个MapReduce Job中完成，输出时两个内容可以混杂在一起。

4. 输入输出文件的格式和其他具体要求请见FTP上“实验要求”文件夹下对应的PDF文档。

## 2 实验思路

倒排索引算法是wordcount的扩展问题，他需要统计一个单词在所有文件中各自出现的次数，直观的，我们可以设计一种Mapper和Reducer

```
class mapper
	for each word in file
		key = word
		value = {filename , 1}

calss reducer
	for each value in Values
		key += {filename, value}
```

但是这种方法过于朴素，为了降低mapper和reducer的传输开销与存储开销，我们应用了combiner在每次mapper结束后进行一次reducer，将结果汇总。

```java
class Mapper
	procedureMap(docid dn, doc d)
        F ← new AssociativeArray
        for all term t ∈doc d do
        	F{t} ← F{t} + 1
        for all term t ∈Fdo
        	Emit(term t, posting <dn, F{t}>)
class Reducer
    procedureReduce(term t, postings [<dn1, f1>, <dn2, f2>…])
        P← new List
        for all posting <dn, f> ∈postings [<dn1, f1>, <dn2, f2>…] do
        	Append(P, <dn, f>)
        Sort(P)
        Emit(term t; postings P)
```

然而，这样会有一个新的问题:

> 当对键值对进行shuffle处理以传送给合适的Reducer时，将按照新的键<t,dn> 进行排序和选择Reducer，因而同一个term的键值对可能被分区到不同的Reducer！

因此，我们需要定制Partitioner来解决这个问题。

```java
Class NewPartitionerextends HashPartitioner<K,V>
// org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
{ // override the method
    getPartition(Kkey, Vvalue, intnumReduceTasks)
    { 
    	term = key.toString().split(“,”)[0]; //<term, docid>=>term
    	super.getPartition(term, value, numReduceTasks);
    }
}
Set the customized partitionerin job configuration
Job. setPartitionerClass(NewPartitioner)
```



## 3 实验代码



1.	程序设计说明 设计思路：分别统计各个文件各行各个词的出现次数，最后汇总 算法设计：map统计输入行各个词的出现次数，在combine的时候将相同词的出现次数合并，partion根据词进行划分，reduce时将各个词在不同文件的出现次数进行汇总 程序主要分为四个部分mapper,combiner, partitioner, reducer, class InvertedIndexMapper（许，杨）：以词为key，出现次数为value建立hash表，对输入行的每个单词判断是否在hash表中，如果在则将出现次数加一，否则插入hash表中置出现次数为1，处理完输入后再hash表转换为（词+“，”+文件名，出现次数）输出 class LineCombiner（许）：将不同行的所有相同词的map输出在本地将出现次数相加，得到的即是词在某个文件的一个map输入的出现次数的输出 class WordPartition（许）：以词而非词加文件名为基准进行划分决定reduce的输入 class InvertedIndexReducer（吴，伍）：在类中使用static的变量prevWord记录上一次输入的词，sum_of_frequency记录词在文件中的总出现次数，num_of_file记录词出现的文件个数，postings记录最后输出的（小说：词频）。因为reduce得到的输入是排过序的，由此对每一个输入判断是否是和上一个词相同，如果不是，输出词和postings，重新设置上述变量值。否则正常更新上述变量 main函数:设置MapReduce各个部分所需的类  



## 4 实验结果

## 5 实验总结



