#include <iostream>
#include <fstream>
#include <sstream>
#include <ctime>
#include <string>
#include "pm_ehash.h"
using namespace std;

void load(PmEHash* ehash){
    ifstream in("");
    if(!in){
        cout << "error opening load file" << endl;
        return;
    }

    string line;
    string opera, temp;
    uint64_t key, value;
    while(getline(in, line)){
        istringstream ss(line);
        ss >> opera;
        ss >> temp;
        istringstream is(temp.substr(0,8));
        is >> key;
        
    }
}

void run(PmEHash* ehash){
    ifstream in("");

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