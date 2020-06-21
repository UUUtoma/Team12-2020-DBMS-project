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
            if(opera == "INSERT") ehash->insert(new_kv);
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
            if(opera == "INSERT") ehash->insert(new_kv);
            else if(opera == "UPDATE") ehash->update(new_kv);
            else if(opera == "READ"){
                uint64_t return_value;
                ehash->search(new_kv.key, return_value);
                cout << return_value << endl;
            }
            else ehash->remove(new_kv.key);
        }
        in.close();
    }
}

int mian(){
    double diff;
    clock_t start, end;
    PmEHash* ehash = new PmEHash;
    start = clock();
    load(ehash);
    run(ehash);
    end = clock();
    diff = (double)(end - start);
    printf("Load time is : %f\n", diff / CLOCKS_PER_SEC);
}