# graph-computation

An opensource distributed GRAph Pattern matching Engine.  
The original project was forked from https://github.com/yecol/exp-dmine  

## Acknowledgement

- Underlying graph structure, JGraphT. http://jgrapht.org/  
- Graph partitioning lib, Metis. http://glaros.dtc.umn.edu/gkhome/views/metis  

### Dataset Introdection  
- ./dataset/liantong_1/* is a small graph made by JunJie Wang and transfered by KeXiang Wang with tools in https://github.com/KeXiangWang/graph_builder  
- ./dataset/pokec/* is a small graph made by JunJie Wang and transfered by JunJie Wang.  
- ./dataset/pokec_0/* is a experimental graph provided by the original project.  

### Instruction
- Check **./run.sh** to know about how to run the engine and what environmant are needed. Besides the file **Maven** is needed.
- Before running, use "mvn install" to pack and compile the project.
- With a prepared environment, run **./auto_run.sh** to automatically run the engine.