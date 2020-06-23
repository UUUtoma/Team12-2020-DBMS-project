#include <iostream>
#include <fstream>
#include <sstream>
#include <ctime>
#include <string>
#include "pm_ehash.h"
using namespace std;

string load_file[] = {"1w-rw-50-50-load.txt","10w-rw-0-100-load.txt","10w-rw-25-75-load.txt",
                    "10w-rw-50-50-load.txt","10w-rw-75-25-load.txt","10w-rw-100-0-load.txt",
                    "220w-rw-50-50-load.txt"};
string run_file[] = {"1w-rw-50-50-run.txt","10w-rw-0-100-run.txt","10w-rw-25-75-run.txt",
                    "10w-rw-50-50-run.txt","10w-rw-75-25-run.txt","10w-rw-100-0-run.txt",
                    "220w-rw-50-50-run.txt"};
double ope[4] = {0};


void get_info(string line, string& opera, kv& new_kv){
    string temp;
    uint64_t key, value;
    istringstream ss(line);
    ss >> opera;
    ss >> temp;
    istringstream is(temp.substr(0,8));
    is >> key;
    istringstream is(temp.substr(8,temp.length()));
    is >> value;
    new_kv.key = key;
    new_kv.value = value;
}

void load(PmEHash* ehash){
    for(int i = 0; i < load_file->length(); i++){
        ifstream in(load_file[i]);
        if(!in){
            cout << "error opening load file" << endl;
            return;
        }

        string line, opera;
        kv new_kv;
        while(getline(in, line)){
            get_info(line, opera, new_kv);
            if(opera == "INSERT"){
                ehash->insert(new_kv);
                ope[0]++;
            }
        }
        in.close();
    }
}

void run(PmEHash* ehash){
    for(int i = 0; i < run_file->length(); i++){
        ifstream in(run_file[i]);
        if(!in){
            cout << "error opening run file" << endl;
            return;
        }

        string line, opera;
        kv new_kv;
        while(getline(in, line)){
            get_info(line, opera, new_kv);
            if(opera == "INSERT"){
                ehash->insert(new_kv);
                ope[0]++;
            }
            else if(opera == "UPDATE"){
                ehash->update(new_kv);
                ope[2]++;
            }
            else if(opera == "READ"){
                uint64_t return_value;
                ehash->search(new_kv.key, return_value);
                cout << return_value << endl;
                ope[3]++;
            }
            else{
                ehash->remove(new_kv.key);
                ope[1]++;
            }
        }
        in.close();
    }
}

int mian(){
    double diff1, diff2;
    clock_t start, middle, end;
    PmEHash* ehash = new PmEHash;
    start = clock();
    load(ehash);
    middle = clock();
    run(ehash);
    end = clock();
    diff1 = (double)(middle - start);
    diff2 = (double)(end - middle);
    cout << "Load time is " << diff1 / CLOCKS_PER_SEC << endl;
    cout << "Run time is " << diff2 / CLOCKS_PER_SEC << endl;
    double total = 0;
    for(int i = 0; i < 4; i++)
        total += ope[i];
    cout << "Total operations : " << total << endl;
    cout << "Insert operation : " << ope[0] / total << endl;
    cout << "Remove operation : " << ope[1] / total << endl;
    cout << "Update operation : " << ope[2] / total << endl;
    cout << "Read operation : " << ope[3] / total << endl;
    cout << "OPS : " << total / ((end - start) / CLOCKS_PER_SEC) << endl;
}