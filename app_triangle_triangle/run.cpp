//########################################################################
//## Copyright 2018 Da Yan http://www.cs.uab.edu/yanda
//##
//## Licensed under the Apache License, Version 2.0 (the "License");
//## you may not use this file except in compliance with the License.
//## You may obtain a copy of the License at
//##
//## //http://www.apache.org/licenses/LICENSE-2.0
//##
//## Unless required by applicable law or agreed to in writing, software
//## distributed under the License is distributed on an "AS IS" BASIS,
//## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//## See the License for the specific language governing permissions and
//## limitations under the License.
//########################################################################

#include "subg-dev.h"

//note for changing to triangle enumeration
//for triangle v1-v2-v3 with v1<v2<v3, we can maintain v1 in task.context if we output triangles

struct AdjItem
{
	VertexID id;
	Label l;
};

struct TriangleTriangleValue
{
	Label l;
	vector<AdjItem> adj;
};

typedef Vertex<VertexID, TriangleTriangleValue> TriangleTriangleVertex;
typedef Subgraph<TriangleTriangleVertex> TriangleTriangleSubgraph;
typedef Task<TriangleTriangleVertex, char> TriangleTriangleTask; //context = step

obinstream & operator>>(obinstream & m, AdjItem & v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ibinstream & operator<<(ibinstream & m, const AdjItem & v)
{
    m << v.id;
    m << v.l;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, AdjItem & v)
{
    m >> v.id;
    m >> v.l;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const AdjItem & v)
{
    m << v.id;
    m << v.l;
    return m;
}

//------------------
obinstream & operator>>(obinstream & m, TriangleTriangleValue & Val)
{
    m >> Val.l;
    m >> Val.adj;
    return m;
}

ibinstream & operator<<(ibinstream & m, const TriangleTriangleValue & Val)
{
    m << Val.l;
    m << Val.adj;
    return m;
}

ofbinstream & operator>>(ofbinstream & m, TriangleTriangleValue & Val)
{
    m >> Val.l;
    m >> Val.adj;
    return m;
}

ifbinstream & operator<<(ifbinstream & m, const TriangleTriangleValue & Val)
{
    m << Val.l;
    m << Val.adj;
    return m;
}
//-------------------
// add a node to graph: only id and label of v, not its edges
// must make sure g.hasVertex(v.id) == true !!!!!!
void addNode(TriangleTriangleSubgraph & g, TriangleTriangleVertex & v)
{
	TriangleTriangleVertex temp_v;
	temp_v.id = v.id;
	temp_v.value.l = v.value.l;
	g.addVertex(temp_v);
}

void addNode(TriangleTriangleSubgraph & g, VertexID id, Label l)
{
	TriangleTriangleVertex temp_v;
	temp_v.id = id;
	temp_v.value.l = l;
	g.addVertex(temp_v);
}

// add a edge to graph
// must make sure id1 and id2 are added first !!!!!!
void addEdge(TriangleTriangleSubgraph & g, VertexID id1, VertexID id2)
{
    TriangleTriangleVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    AdjItem temp_adj;
	temp_adj.id = v2->id;
	temp_adj.l = v2->value.l;
	v1->value.adj.push_back(temp_adj);
	temp_adj.id = v1->id;
	temp_adj.l = v1->value.l;
	v2->value.adj.push_back(temp_adj);
}

void addEdge_safe(TriangleTriangleSubgraph & g, VertexID id1, VertexID id2) //avoid redundancy
{
    TriangleTriangleVertex * v1, * v2;
    v1 = g.getVertex(id1);
    v2 = g.getVertex(id2);
    int i = 0;
    vector<AdjItem> & adj = v2->value.adj;
    for(; i<adj.size(); i++)
    	if(adj[i].id == id1) break;
    if(i == adj.size())
    {
    	AdjItem temp_adj;
		temp_adj.id = v2->id;
		temp_adj.l = v2->value.l;
		v1->value.adj.push_back(temp_adj);
		temp_adj.id = v1->id;
		temp_adj.l = v1->value.l;
		v2->value.adj.push_back(temp_adj); // bigraph
    }
}

struct less_than_key
{
    inline bool operator() (const TriangleTriangleValue& struct1, const TriangleTriangleValue& struct2)
    {
        return (struct1.id < struct2.id);
    }
};

class TriangleTriangleTrimmer:public Trimmer<TriangleTriangleVertex>
{
    virtual void trim(TriangleTriangleVertex & v) {
    	TriangleTriangleValue & val = v.value.adj;
    	TriangleTriangleValue newval;
        for (int i = 0; i < val.size(); i++) {
            if (v.value.id < val[i].id)
            	newval.push_back(val[i]);
        }
        val.swap(newval);
        sort(val.begin(), val.end(), less_than_key());
    }
};

class TriangleTriangleAgg:public Aggregator<size_t, size_t, size_t>  //all args are counts
{
private:
	size_t count;
	size_t sum;

public:

    virtual void init()
    {
    	sum = count = 0;
    }

    virtual void init_udf(size_t & prev) {
    	sum = 0;
    }

    virtual void aggregate_udf(size_t & task_count)
    {
    	count += task_count;
    }

    virtual void stepFinal_udf(size_t & partial_count)
    {
    	sum += partial_count; //add all other machines' counts (not master's)
    }

    virtual void finishPartial_udf(size_t & collector)
    {
    	collector = count;
    }

    virtual void finishFinal_udf(size_t & collector)
    {
    	sum += count; //add master itself's count
    	if(_my_rank == MASTER_RANK) cout<<"Triangle-Triangle Count = "<<sum<<endl;
    	collector = sum;
    }
};

class TriangleTriangleComper:public Comper<TriangleTriangleTask, TriangleTriangleAgg>
{
public:

	//check whether task is bigtask
	virtual bool is_bigtask(TriangleTriangleTask * task){
		if(task->subG.vertexes.size() > BIGTASK_THRESHOLD
					|| task->to_pull.size() > BIGTASK_THRESHOLD)
			return true;
		else
			return false;
	}

    virtual bool task_spawn(VertexT * v)
    {
    	if(v->value.adj.size() < 2) return false;
    	//cout<<v->id<<": in task_spawn"<<endl;//@@@@@@@@@@@@@
    	TriangleTriangleTask * t = new TriangleTriangleTask;
        addNode(t->subG, *v, 'a'); // a
        for(int i=0; i<v->value.adj.size(); i++) //-1 since we do not need to pull the largest vertex
        {
            VertexID nb = v->value.adj[i];
            t->pull(nb);
        }
        t->context = 1;
        bool result = is_bigtask(t);
        add_task(t);
        return result;
    }

	//input adj-list
	//(1) must be sorted !!! toVertex(v) did it
	//(2) must remove all IDs less than vid !!!
	//trimmer guarantees them
	size_t triangle_triangle_count(SubgraphT & g, vector< *> & frontier)
	{
        
	}

    virtual bool compute(SubgraphT & g, ContextT & context, vector<VertexT *> & frontier)
    {
        if(context == 1)
        {
            // access subG
            VertexID rootID = g.vertexes[0].id; // root = a
            // process frontier
            hash_set<VertexID>  pull_list;
            for (int j = 0; j < g.vertexes[0].adj.size(); j++)
            {
                pull_list.insert(g.vertexes[0].adj[j].id);
                addEdge_safe(g, g.vertexes[0].adj[j].id, rootID);
            }

            TriangleTriangleValue vlist;
            for (int j = 0; j < frontier.size(); j++)
                vlist.push_back(frontier[j]->id);
            for (int j = 0; j < vlist.size(); j++)
            {
                VertexID u = vlist[j].id;
                int m = j + 1;
                TriangleTriangleValue & ulist = frontier[j]->value.adj;
                int k = 0;
                while (k < ulist.size() && m < vlist.size())
                {
                    if (ulist[k].id == vlist[m].id)
                    {
                        // pull the neighbors of u and u''s neighbors
                        for (int l = 0; l < vlist[j].adj.size(); l++)
                        {
                            pull_list.insert(vlist[j].adj[l].id); // d
                            addEdge_safe(vlist[j].adj[l].id, vlist[j].id); // add d to a/b/c for later calculate the degree of d
                        }
                        for (int l = 0; l < ulist[k].adj.size(); l++)
                        {
                            pull_list.insert(ulist[k]].adj[l].id); // d
                            addEdge_safe(ulist[k]].adj[l].id, ulist[k].id); // add d to a/b/c for later calculate the degree of d
                        }
                        addNode(g, u, 'b');
                        vlist[j].value.l = 'b';
                        addNode(g, ulist[k].id, 'c');
                        ulist[k].value.l = 'c'; // should change the label of explored vertexes !!!
                        /*
                        addEdge(g, rootID, u);
                        addEgde(g, ulist[k].id);
                        addEdge(g, u, ulisk[k].id);
                        */
                        m++;
                        k++;
                    }
                    else if (ulisk[k].id > vlist[m].id) m++; // adj_list sorted
                    else k++; // adj_list sorted
                }
                
            }
            // pull list
            for (auto it = pull_list.begin(); it != pull_list.end(); it++) 
            {
                pull(*it);
                addNode(g, *it, 'd');
            }
            
            cout << rootID << ": step 1 done" << endl; // debug

            context++;
            return true;
        }
        else if (context == 2)
        {
            size_t count = 0;
            // access d in subgraph g
            for(int i=0; i<g.vertexes.size(); i++)
            {
                VertexT & v_d = g.vertexes[i];
                if (v_d.value.l == 'd') 
                {
                    size_t triange_count = 0;
                    TriangleTriangleValue vlist;
                    for (int j = 0; j < v_d.value.adj.size(); j++)
                        vlist.push_balck(v_d.value.adj[j].id);
                    for (int j = 0; j < vlist.size() - 1; j++) // -1 because do not need to consider the largest adjacent
                    {
                        VertexID u = vlist[j].id;
                        int m = 0; // m is vlist's starting position to check
                        for (int l = 0; l < frontier.size(); l++)
                            if (frontier[l]->id == u)
                            {
                                TriangleTriangleValue & ulist = frontier[l]->value; // ... should be the index of vertex u in frontier
                                break;
                            }
                        int k = 0; // k is ulist's starting position to check
                        while(k<ulist.size() && m<vlist.size())
                        {
                            if (ulist[k].value.l <= 'c') continue;
                            if (vlist[m].value.l <= 'c') continue;
                            if(ulist[k].id == vlist[m].id)
                            {
                                triange_count++;
                                m++;
                                k++;
                            }
                            else if(ulist[k].id > vlist[m].id) m++;
                            else k++;
                        }
                    }
                    // one d can connect to multiple a or b or c, thus has multiple times contribution to the result.
                    count += triange_count * v_d.value.adj.size(); 
                }
            }

             
            TriangleAgg* agg = get_aggregator();
            agg->aggregate(count);

            cout<<rootID<<": step 2 done"<<endl;
            
            return false;
        }
    }
};

class TriangleTriangleWorker:public Worker<TriangleTriangleComper>
{
public:
	TriangleTriangleWorker(int num_compers) : Worker<TriangleTriangleComper>(num_compers){}

    virtual VertexT* toVertex(char* line)
    {
        VertexT* v = new VertexT;
        char * pch;
        pch=strtok(line, " \t");
        v->id=atoi(pch);
        strtok(NULL," \t");
        TriangleTriangleValue & val = v->value.adj;
        while((pch=strtok(NULL, " ")) != NULL)
        {
            val.push_back(AdjItem(atoi(pch), 'z'));
        }
        return v;
    }

    virtual void task_spawn(VertexT * v, vector<TriangleTriangleTask*> & tcollector)
	{
    	if(v->value.size() < 2) return;
    	TriangleTriangleTask* task = new TriangleTriangleTask;
		task->subG.addVertex(*v);
		for(int i=0; i<v->value.size(); i++) //-1 since we do not need to pull the largest vertex
		{
			VertexID nb = v->value[i];
			task->pull(nb);
		}
		task->context = v->value.back();
		tcollector.push_back(task);
	}
};

int main(int argc, char* argv[])
{
    init_worker(&argc, &argv);
    WorkerParams param;
    if(argc != 4){
		cout<<"arg1 = input path in HDFS, arg2 = number of threads, arg3 = BIGTASK_THRESHOLD"<<endl;
		return -1;
	}
    param.input_path = argv[1];  //input path in HDFS
    int thread_num = atoi(argv[2]);  //number of threads per process
    BIGTASK_THRESHOLD = atoi(argv[3]);
    param.force_write=true;
    param.native_dispatcher=false;
    //------
    TriangleTriangleTrimmer trimmer;
    TriangleTriangleAgg aggregator;
    TriangleTriangleWorker worker(thread_num);
    worker.setTrimmer(&trimmer);
    worker.setAggregator(&aggregator);
    auto start = chrono::high_resolution_clock::now();
    worker.run(param);
    worker_finalize();
    auto stop = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(stop - start);
	cout << "running time(us) = " << duration.count() << endl;
    return 0;
}
