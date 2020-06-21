#include <iostream>
#include <ctime>
using namespace std;

void load(){

}

void run(){

}

int mian(){
    double diff;
    clock_t start, end;
    start = clock();
    load();
    run();
    end = clock();
    diff = (double)(end - start);
    printf("Load time is : %f\n", diff / CLOCKS_PER_SEC);
}