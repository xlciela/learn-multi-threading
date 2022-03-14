/*
HELLO WORLD VERSION MULTITHREADING
Version A02: Using functions allowing passing 2 arguments
*/


#include <iostream>
#include <boost/thread.hpp>
using namespace std;



void doTask(char const* message, int number) {
    cout << message << " " << number << endl;
}



int main() {
    boost::thread th(doTask, "Good day", 19);
    th.join();
    return 0;
}
