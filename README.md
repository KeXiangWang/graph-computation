# graph-computation

An open-source distributed Graph Pattern mining Engine.  
The original project was forked from https://github.com/yecol/exp-dmine  

## Acknowledgement  

- Underlying graph structure, JGraphT. http://jgrapht.org/  
- Graph partitioning lib, Metis. http://glaros.dtc.umn.edu/gkhome/views/metis  

## Updated Part  

- Extend the compatibility to general graphs instead of given special graphs.
- Fix some bugs in Partition.java and DiscoveryTask.java. But the project can not run absolutely right.
- Muted the function of checking source vertex's label and target vertex's label of an edge in method `matchR()` and `matchQ()`.
- In the method `expand()`, banned the part for expanding only from *KV.Person_Label* and changed the condition of general expending.

### Dataset Introdection  

- ./dataset/liantong_small/* is a small graph made by JunJie Wang and transfered by KeXiang Wang with tools in https://github.com/KeXiangWang/graph_builder  
- ./dataset/pokec/* is a small graph made by JunJie Wang and transfered by JunJie Wang.  

### Instruction  

- Check **./run.sh** to know about how to run the engine and what environmant are needed. Besides the file **Maven** is needed.
- Before running, use "mvn install" to pack and compile the project.
- With a prepared environment, run **./auto_run.sh** to automatically run the engine. (The scripts only works for version 0.1)