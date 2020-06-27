#include <iostream>
#include <fstream>
#include <sstream>
#include <ctime>
#include <string>
#include <queue>
#include "pm_ehash.h"
#include "data_page.h"
using namespace std;

//load、run文件
string load_file[] = {"1w-rw-50-50-load.txt","10w-rw-0-100-load.txt","10w-rw-25-75-load.txt",
                    "10w-rw-50-50-load.txt","10w-rw-75-25-load.txt","10w-rw-100-0-load.txt",
                    "220w-rw-50-50-load.txt"};

string run_file[] = {"1w-rw-50-50-run.txt","10w-rw-0-100-run.txt","10w-rw-25-75-run.txt",
                    "10w-rw-50-50-run.txt","10w-rw-75-25-run.txt","10w-rw-100-0-run.txt",
                    "220w-rw-50-50-run.txt"};
//每个操作的数量，分别为insert、remove、update、read
double ope[4] = {0};
//每个操作的运行总时间
double t[4] = {0};
//load、run文件目录
string dir = "../../workloads/";
//存储每个文件的运行时间
queue<double> time_queue;



/**
 * @description: 读入一行，返回包含的操作类型和kv
 * @param string: 从文件读入的一行
 * @param string&: 返回的操作类型
 * @param kv&: 返回的new_kv键值对
 * @return: NULL
 */
void get_info(string line, string& opera, kv& new_kv){
    string temp;
    uint64_t key, value;
    istringstream ss(line);
    ss >> opera;
    ss >> temp;
    istringstream a(temp.substr(0,8));
    a >> key;
    istringstream b(temp.substr(8,temp.length()));
    b >> value;
    new_kv.key = key;
    new_kv.value = value;
}



/**
 * @description: 执行load阶段操作
 * @param PmEHash*: PmEHash实例
 * @return: NULL
 */
void load(PmEHash* ehash){

    for(int i = 0; i < sizeof(load_file)/sizeof(load_file[0]); i++){
        clock_t start = clock();
        ifstream in(dir + load_file[i]);
        if(!in){
            cout << "error opening load file" << endl;
            return;
        }

        string line, opera;
        kv new_kv;
        //对每一行执行get_info()并执行insert操作
        while(getline(in, line)){
            get_info(line, opera, new_kv);
            if(opera == "INSERT"){
                ehash->insert(new_kv);
                ope[0]++;
            }
            if(in.fail()){
                break;
            }
        }
        in.close();

        clock_t end = clock();
        double time = (double)(end - start) / CLOCKS_PER_SEC;
        time_queue.push(time);
    }
}



/**
 * @description: 执行run阶段操作
 * @param PmEHash*: PmEHash实例
 * @return: NULL
 */
void run(PmEHash* ehash){

    for(int i = 0; i < sizeof(run_file)/sizeof(run_file[0]); i++){
        clock_t start = clock();
        ifstream in(dir + run_file[i]);
        if(!in){
            cout << "error opening run file" << endl;
            return;
        }

        string line, opera;
        kv new_kv;
        clock_t s, e;
        while(getline(in, line)){
            get_info(line, opera, new_kv);
            if(opera == "INSERT"){
                s = clock();
                ehash->insert(new_kv);
                e = clock();
                t[0] += (double)(e - s) / CLOCKS_PER_SEC;
                ope[0]++;
            }

            else if(opera == "UPDATE"){
                s = clock();
                ehash->update(new_kv);
                e = clock();
                t[2] += (double)(e - s) / CLOCKS_PER_SEC;
                ope[2]++;
            }
            else if(opera == "READ"){
                uint64_t return_value;
                s = clock();
                ehash->search(new_kv.key, return_value);
                e = clock();
                t[3] += (double)(e - s) / CLOCKS_PER_SEC;
                cout << return_value << endl;
                ope[3]++;

            }
            else{
                s = clock();
                ehash->remove(new_kv.key);
                e = clock();
                t[1] += (double)(e - s) / CLOCKS_PER_SEC;
                ope[1]++;
            }
            if(in.fail()) 
                break;
        }
        in.close();

        clock_t end = clock();
        double time = (double)(end - start) / CLOCKS_PER_SEC;
        time_queue.push(time);
    }
}



int main(){
    double diff1, diff2;
    clock_t start, middle, end;
    PmEHash* ehash = new PmEHash;
    double speed[4];

    start = clock();
    load(ehash);
    middle = clock();
    run(ehash);
    end = clock();

    diff1 = (double)(middle - start);
    diff2 = (double)(end - middle);
    double total = 0;
    for(int i = 0; i < 4; i++)
        total += ope[i];
    t[0] += diff1 / CLOCKS_PER_SEC;
    for(int i = 0; i < 4; i++){
        if(t[i] == 0) speed[i] = 0;
        else speed[i] = ope[i] / t[i];
    }
        
    cout << "Load time is " << diff1 / CLOCKS_PER_SEC << " seconds" << endl;
    cout << "Run time is " << diff2 / CLOCKS_PER_SEC << " seconds" << endl;

    cout << "Total operation number: " << total << endl;
    cout << "Insert operation ratio: " << ope[0] / total << endl;
    cout << "Remove operation ratio: " << ope[1] / total << endl;
    cout << "Update operation ratio: " << ope[2] / total << endl;
    cout << "Read operation ratio: " << ope[3] / total << endl;
    cout << "OPS : " << total / ((end - start) / CLOCKS_PER_SEC) << endl;

    cout << "Load file running time : " << endl;
    for(int i = 0; i < sizeof(load_file)/sizeof(load_file[0]); i++){
        cout << load_file[i] << " : " << time_queue.front() << " seconds" << endl;
        time_queue.pop();
    }

    cout << "Run file running time : " << endl;
    for(int i = 0; i < sizeof(run_file)/sizeof(run_file[0]); i++){
        cout << run_file[i] << " : " << time_queue.front() << " seconds" << endl;
        time_queue.pop(); 

    }
    cout << "Speed of operations: " << endl;
    cout << "Insert oparation's speed : " << speed[0] << " ope per second" << endl;
    cout << "Remove oparation's speed : " << speed[1] << " ope per second" << endl;
    cout << "Update oparation's speed : " << speed[2] << " ope per second" << endl;
    cout << "Read oparation's speed : " << speed[3] << " ope per second" << endl;

    ehash->selfDestory();

    return 0;
}
