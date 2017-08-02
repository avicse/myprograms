package myprojbda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class GraphAdjacencyList
{
    Map<Integer, List<Integer>> Adjacency_List;	
    public GraphAdjacencyList(int number_of_vertices)
    {
        Adjacency_List = new HashMap<Integer, List<Integer>>();	
        for (int i = 1 ; i <= number_of_vertices ; i++)
        { 
            Adjacency_List.put(i, new LinkedList<Integer>());
        }
    }
    public void setEdge(int source , int destination)
    {
       if (source > Adjacency_List.size() || destination > Adjacency_List.size())
       {
           System.out.println("the vertex entered in not present ");
           return;
       }
       List<Integer> slist = Adjacency_List.get(source);
       slist.add(destination);
       List<Integer> dlist = Adjacency_List.get(destination);
       dlist.add(source);
    }
    
    public List<Integer> getEdge(int source)
    {
        if (source > Adjacency_List.size())
        {
            System.out.println("the vertex entered is not present");
            return null;
        }
        return Adjacency_List.get(source);
    }
    
    int countingTraingles(String filename) throws Exception{
        
        int source, destination, count = 0;
        String s;
        int number_of_vertices;
        number_of_vertices = 4036530;//Integer.parseInt(args[1]);
        try
        {
            GraphAdjacencyList adjacencyList = new GraphAdjacencyList(number_of_vertices);
            File file = new File(filename);
            BufferedReader br = new BufferedReader(new FileReader(file));
            while((s = br.readLine())!= null){
                source = Integer.parseInt(s.split("	")[0]);
                destination =Integer.parseInt(s.split("	")[1]);
                adjacencyList.setEdge(source, destination);    
            }
            br.close();
            /* Prints the adjacency List representing the graph.*/
                     
            System.out.println ("the given Adjacency List for the graph \n");
            for (int i = 1 ; i <= number_of_vertices ; i++)
            {
                System.out.print(i+"->");
                List<Integer> edgeList = adjacencyList.getEdge(i);
                for (int j = 1 ; ; j++ )
                {
                    if (j != edgeList.size())
                    {
                        System.out.print(edgeList.get(j - 1 )+"->");
                    }else
                    {
                        System.out.print(edgeList.get(j - 1 ));
                        break;
                    }						 
                }
                System.out.println();					
            } 
            
            File file1 = new File(filename);
            BufferedReader br1 = new BufferedReader(new FileReader(file));
            while((s = br1.readLine())!= null){
                source = Integer.parseInt(s.split(" ")[0]);
                destination =Integer.parseInt(s.split(" ")[1]);
                adjacencyList.setEdge(source, destination);    
                /*
                for(int i=1;i<=15;i++){
                    if(adjacencyList.Adjacency_List.get(i).contains(source) & adjacencyList.Adjacency_List.get(i).contains(destination)){
                        count++;
                        System.out.println("Trangle : "+i+"-"+source+"-"+destination);
                    }
                } */
            }
        } 
        catch(InputMismatchException inputMismatch)
        {
            System.out.println("Error in Input Format. \nFormat : <source index> <destination index>");
        }
        return count/3;
    }   
    
}



class NodeLists
{
    List l1;
    List l2;
    List l3;
    List l4;
    List l5;
    String reducerKeys[];
    int p,q;
    HashMap<String,Integer> setReduceMap;
    NodeLists(int p,int q)
    {
        this.p = p;
        this.q = q;
       // Create 5 lists
        l1 = new ArrayList();
        l2 = new ArrayList();
        l3 = new ArrayList();
        l4 = new ArrayList();
        l5 = new ArrayList();
     
        for(int i=1;i<=4036530;i++)
            
            if ((i%5)==1){
                l1.add(i);
            }
            else if ((i%5)==2){
                l2.add(i);
            }
            else if ((i%5)==3){
                l3.add(i);
            }
            else if((i%5)==4){
                l4.add(i);
            }
            else if((i%5)==0){
                l5.add(i);
            }
        
        reducerKeys = new String[100];
        int l=1,n=5;
        for(int i=1;i<=(n-2);i++)
        {
            for(int j=i+1;j<=(n-1);j++)
            {
                for(int k=j+1;k<=(n);k++)
                {
                    String x;
                    x=(i+""+j+""+k);
                    reducerKeys[l] = x;
                    l++;
                }
            }
        }
       // printReducers();        
    }
    
    char existedListIndex(int value){
        if(l1.contains(value))
            return '1';
        else if(l2.contains(value))
            return '2';
        else if(l3.contains(value))
            return '3';
        else if(l4.contains(value))
            return '4';
        else 
            return '5';
    }
    
    void printReducers()
    {
        char pListIndex,qListIndex;
        
        pListIndex = existedListIndex(p);
        qListIndex = existedListIndex(q);
        System.out.println("These Nodes belong to:");
        System.out.println("List:"+pListIndex);
        System.out.println("List:"+qListIndex);
        for(int i = 1;i<=10;i++){
           if( reducerKeys[i].contains(Character.toString(pListIndex)) & reducerKeys[i].contains(Character.toString(qListIndex)) )
           {
                System.out.println(reducerKeys[i]);
           }
        }
    }
}

class Edge
{
    int u,v;
    Edge(int u,int v){
        this.u = u;
        this.v = v;
    }
    Edge(){
        u = 0;
        v = 0;
    }
    void setEdge(int u,int v){
        this.u = u;
        this.v = v;
    }
    int getFirstVertix(){
        return u;
    }
    
    int getSecondtVertix(){
        return v;
    }
}

public class HadoopTC {
    
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text edge, Context con) throws IOException, InterruptedException {
            String[] word = edge.toString().split("\t");
            NodeLists eRed = new NodeLists(Integer.parseInt(word[0]),Integer.parseInt(word[1]));
            try{
                char uListIndex,vListIndex;
                uListIndex = eRed.existedListIndex(eRed.p); 
                vListIndex = eRed.existedListIndex(eRed.q);

                for(int i = 1;i<=10;i++){
                    if( eRed.reducerKeys[i].contains(Character.toString(uListIndex)) & eRed.reducerKeys[i].contains(Character.toString(vListIndex)) )
                    {
                        //Edge ed = new Edge(Integer.parseInt(word[0]),Integer.parseInt(word[1]));
                        //con.write(new Text(eRed.reducerKeys[i]),ed);
                        con.write(new Text(eRed.reducerKeys[i]),new Text(word[0]+","+word[1]));
                    }
                }
            } 
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> valueList,Context con) throws IOException, InterruptedException {
            GraphAdjacencyList adjacencyList =new GraphAdjacencyList(4036535);
            int source,destination;
            
            try {
                int count = 0;
                for (Text var : valueList) {
                    source = Integer.parseInt(var.toString().split(",")[0]);
                    destination =Integer.parseInt(var.toString().split(",")[1]);
                    adjacencyList.setEdge(source, destination);
                    for(int i=1;i<=4036530;i++){
                        if(adjacencyList.Adjacency_List.get(i).contains(source) & adjacencyList.Adjacency_List.get(i).contains(destination)){
                            count++;
                            //System.out.println("Trangle : "+i+"-"+source+"-"+destination);
                        }
                    }
                }        
                
                
                String out = "Count: " +count;
                con.write(key, new Text(out));
            } 
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf, "FindSalary Groups");
            job.setJarByClass(HadoopTC.class);
  job.setMapperClass(MapperClass.class);
  job.setReducerClass(ReducerClass.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  
  Path pathInput = new Path(args[0]);//"emprecords.txt");
  Path pathOutputDir = new Path(args[1]);//"/home/avinashp/Desktop/empout.txt");
  FileInputFormat.addInputPath(job, pathInput);
  FileOutputFormat.setOutputPath(job, pathOutputDir);
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 } catch (IOException e) {
  e.printStackTrace();
 } catch (ClassNotFoundException e) {
  e.printStackTrace();
 } catch (InterruptedException e) {
  e.printStackTrace();
 }

}
    
}