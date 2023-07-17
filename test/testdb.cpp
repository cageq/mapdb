
#include "mapdb.h"
#include <string> 
#include <iostream>
int main(int argc, char * argv[]){

    MapDB<int32_t, std::string, std::string> myMap; 

    for(int32_t i = 1;i < 10;i ++){
        auto key2 = "key" + std::to_string(i); 
        myMap.insert(i, key2, "world" + std::to_string(i)); 
    }


    for(int32_t i = 1;i < 10;i ++){
        myMap.insert(i, "hello" + std::to_string(i +1 ), "world" + std::to_string(10000+ i)); 
    }

    for(auto itr = myMap.begin(); itr != myMap.end(); itr ++){
        std::cout << "data key (" << itr->first.first << "," << itr->first.second << ")\n"; 

    }


    auto rst = myMap.find_by_key1(5); 
    for (auto itr = rst.first;  itr != rst.second; itr ++){
        std::cout << " key (" << itr->first << "," << itr->second->key2 << ")  value " << itr->second->value  << "\n"; 
    }

    for (auto itr = rst.first;  itr != rst.second; itr ++){
        myMap.update(itr->first, itr->second->key2, "updated " ); 
    }


    std::cout << "updated ==========================" << std::endl; 

    for(auto itr = myMap.begin(); itr != myMap.end(); itr ++){
        std::cout << "data key (" << itr->first.first << "," << itr->first.second << ")  value " << itr->second->value << "\n"; 

    }


    // insert roll back 
    std::cout << "insert rollback ==========================" << std::endl; 
    myMap.begin_transaction(); 

    auto rst6 = myMap.find_by_key1(6); 
    for (auto itr = rst6.first;  itr != rst6.second; itr ++){
        myMap.update(itr->first, itr->second->key2, "updated 666666666" ); 
    }
    myMap.insert(111, "hello111", "insert 111"); 
    for(auto itr = myMap.begin(); itr != myMap.end(); itr ++){
        std::cout << "data key (" << itr->first.first << "," << itr->first.second << ")  value " << itr->second->value << "\n";          
    }

    myMap.rollback(); 

    std::cout << "after rollback : " << myMap.size()  << std::endl; 
    for(auto itr = myMap.begin(); itr != myMap.end(); itr ++){
        std::cout << "data key (" << itr->first.first << "," << itr->first.second << ")  value " << itr->second->value << "\n";          
    }


    std::cout << "delete  rollback ==========================" << std::endl; 
    myMap.begin_transaction(); 
    myMap.remove(8, "hello9");
    std::cout << "after remove 8   ==========================" << std::endl; 
    for(auto itr = myMap.begin(); itr != myMap.end(); itr ++){
        std::cout << "data key (" << itr->first.first << "," << itr->first.second << ")  value " << itr->second->value << "\n";          
    }
    myMap.commit(); 
    myMap.rollback(); 
    std::cout << "after rollback 8   ==========================" << std::endl; 
    for(auto itr = myMap.begin(); itr != myMap.end(); itr ++){
        std::cout << "data key (" << itr->first.first << "," << itr->first.second << ")  value " << itr->second->value << "\n";          
    }

}

