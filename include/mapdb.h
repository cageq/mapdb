// created by arthur @2023 0706

#pragma once 

#include <inttypes.h>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector> 
#include <mutex>
#include <atomic> 
#include <stack> 
#include <algorithm>
#include <iostream> 

struct EmptyMutex{
    void lock(){}
    void unlock(){}
}; 


template <typename Key1, typename Key2, typename Val , typename Mutex = EmptyMutex> 
class MapDB{
    public: 

    using Key = std::pair<Key1, Key2>; 

    struct Record{        
        Key1  key1; 
        Key2  key2; 
        Val  value; 
    }; 
    enum TransactionType{
        TRANSACTION_INSERT, 
        TRANSACTION_UPDATE, 
        TRANSACTION_DELETE, 
    }; 

    struct Transaction{
        TransactionType type; 
        Record * record; 
    }; 
    
    using iterator = typename  std::map<Key, Record * >::iterator; 

    iterator begin()  {
        return data_map.begin(); 
    }

    iterator end()   {
        return data_map.end(); 
    }


    using IndexKey1 = std::multimap<Key1, Record *  >; 
    using IndexKey2 = std::multimap<Key2, Record *  >; 
 
    	void begin_transaction()
		{			 
            std::lock_guard<Mutex> lock(data_mutex);
			has_transaction = true;		 
		}
		
		void rollback()
		{		 
            data_mutex.lock(); 
			if (!has_transaction)
			{ 
                data_mutex.unlock(); 
				return;
			}
            
			while (!trans_stack.empty())
			{      
                auto  trans = trans_stack.top();
                trans_stack.pop();
                data_mutex.unlock(); 

                auto key = std::make_pair(trans.record->key1, trans.record->key2);
                switch(trans.type ){
                    case TRANSACTION_INSERT:
                    {
                        
                        {
                            std::lock_guard<Mutex> lock(index_mutex);
                            //remove index 
                            auto range1 = index_key1.equal_range(trans.record->key1);
                            auto key1Itr  = std::find_if(range1.first, range1.second , [&]( auto idxitr){
                                    return idxitr.second->key2 == trans.record->key2;  
                                }); 
                            index_key1.erase(key1Itr); 

                            auto range2 = index_key2.equal_range(trans.record->key2);
                            auto key2Itr  = std::find_if(range2.first, range2.second,  [&]( auto idxitr){
                                    return idxitr.second->key1 == trans.record->key1;  
                                } );                              
                            index_key2.erase(key2Itr); 
                        }
                      

                        // std::cout << "rollback key " << key.first << ":" << key.second << std::endl; 
                        {
                            std::lock_guard<Mutex> lock(data_mutex);
                            auto itr = data_map.find(key); 
                            if (itr != data_map.end()){
                                delete itr->second; 
                                data_map.erase(itr); 
                            }
                        }
                 
                    }
                    break; 
                    case TRANSACTION_UPDATE: 
                    {
                        std::lock_guard<Mutex> lock(data_mutex);
                        auto itr = data_map.find(key); 
                        if (itr != data_map.end()){
                            //update to old value 
                            itr->second->value = trans.record->value; 
                        }
                        delete trans.record; 
                    }
                    break; 

                    case TRANSACTION_DELETE: 
                    {
                        //reinsert again 
                        {
                            std::lock_guard<Mutex> lock(data_mutex);
                            data_map[key] = trans.record; 
                        }                      
                        
                        
                        {
                            std::lock_guard<Mutex> lock(index_mutex);
                            index_key1.emplace(std::make_pair(trans.record->key1, trans.record )); 
                            index_key2.emplace(std::make_pair(trans.record->key2, trans.record ));  
                        }
                        
                    }
                
                    break; 
                    default: 
                    ; 
                }  	
                data_mutex.lock(); 	
			}
			has_transaction = false;	 
            data_mutex.unlock(); 
		}

		void commit()
		{	 
            std::lock_guard<Mutex> lock(data_mutex);
			if (!has_transaction)
			{			 
				return;
			}
			has_transaction = false;  

            while (!trans_stack.empty())
			{            
                auto  trans = trans_stack.top();
                switch(trans.type ){
                    case TRANSACTION_UPDATE:
                    {
                        delete trans.record; 
                    }
                    break; 
                    case TRANSACTION_DELETE:
                    {
                        delete trans.record; 
                    }
                    break; 
                    default:
                    ; 
                }
                trans_stack.pop(); 
            }
           		            	
		}
    
        bool insert(Key1 key1,Key2 key2,  Val val ){
            std::cout << "insert key1 " << key1  << " key2 " << key2 << std::endl; 
            Record * row = new Record{key1,key2 , val}; 

            auto key = std::make_pair(key1, key2);
            {
                std::lock_guard<Mutex> lock(data_mutex);
                auto itr = data_map.find(key); 
                if (itr != data_map.end()){
                    //key existed 
                    return false;
                }
          
                data_map[key] = row; 

                if (has_transaction)
                {
                    trans_stack.push(Transaction{TRANSACTION_INSERT, row});   
                }
            }
                
            {
                std::lock_guard<Mutex> lock(index_mutex);
                index_key1.emplace(std::make_pair(key1, row )); 
                index_key2.emplace(std::make_pair(key2, row ));  
            } 
        
        return true; 
    }

    bool  update(Key1 key1, Key2 key2, Val value)
    {        

        std::cout << "update key1 " << key1  << " key2 " << key2 << std::endl; 
        auto key = std::make_pair(key1, key2);
        std::lock_guard<Mutex> lock(data_mutex);
        auto itr = data_map.find(key);   
        if (itr != data_map.end()){
            if (has_transaction)
            {                     
                Record * oldRecord  = new Record{key1,key2 , itr->second->value };    
                trans_stack.push(Transaction{TRANSACTION_UPDATE, oldRecord});   
            }  
            //just update value 
            itr->second->value = value; 
            return true; 
        }     
        return false; 
	}

     bool  update_or_insert(Key1 key1, Key2 key2, Val value)
     {
        std::cout << "update or insert  key1 " << key1  << " key2 " << key2 << std::endl; 
        auto key = std::make_pair(key1, key2);
        do{
            std::lock_guard<Mutex> lock(data_mutex);
            auto itr = data_map.find(key);   
            if (itr != data_map.end()){
                if (has_transaction)
                {                     
                    Record * oldRecord  = new Record{key1,key2 , itr->second->value };    
                    trans_stack.push(Transaction{TRANSACTION_UPDATE, oldRecord});   
                }  
                //just update value 
                itr->second->value = value; 
                return true; 
            }    
        }while(0);             
        
        //no record 
        {
            Record * row = new Record{key1,key2 , value};     
            {
                std::lock_guard<Mutex> lock(data_mutex);                   
                data_map[key] = row; 
                if (has_transaction)
                {
                    trans_stack.push(Transaction{TRANSACTION_INSERT, row});   
                }
            }
                
            {
                std::lock_guard<Mutex> lock(index_mutex);
                index_key1.emplace(std::make_pair(key1, row )); 
                index_key2.emplace(std::make_pair(key2, row ));  
            } 

        }        
        return false; 
     }

    bool  remove(Key1 key1, Key2 key2 ){
        bool hasRemoved = false; 
        auto key = std::make_pair(key1, key2);

        {
            std::lock_guard<Mutex> lock(data_mutex);
            auto itr = data_map.find(key);   
            if (itr != data_map.end()){                  
                
                data_map.erase(itr);   
                if (has_transaction)  {                                     
                    trans_stack.push(Transaction{TRANSACTION_DELETE, itr->second });                   
                }  else {
                    delete itr->second; 
                }            
                hasRemoved = true;  
            }     
        }     


        {
            std::lock_guard<Mutex> indexLock(index_mutex);
            //remove index 
            auto range1 = index_key1.equal_range(key1);
            auto key1Itr  = std::find_if(range1.first, range1.second , [&]( auto idxitr){
                    return idxitr.second->key2 == key2;  
                }); 
            index_key1.erase(key1Itr); 

            auto range2 = index_key2.equal_range(key2);
            auto key2Itr  = std::find_if(range2.first, range2.second,  [&]( auto idxitr){
                    return idxitr.second->key1 == key1;  
                } );                              
            index_key2.erase(key2Itr); 
        }
       
            
    
        return hasRemoved;   
    }

    Val*  find(Key1 key1, Key2 key2)
    {	
        std::lock_guard<Mutex> lock(data_mutex);

        auto itr = data_map.find(std::make_pair(key1, key2));  
        if (itr != data_map.end()){
            return &(itr->second->value); 
        }
        return nullptr; 
    }


    size_t size() const {
        
        return data_map.size(); 
    }


    bool has_key(Key1 key1, Key2 key2)
    {
        std::lock_guard<Mutex> lock(data_mutex);
        return data_map.find(std::make_pair(key1, key2)) != data_map.end();
    }

    const std::pair<typename IndexKey1::iterator,typename IndexKey1::iterator> find_by_key1(const Key1 &  key)
    {
        std::lock_guard<Mutex> lock(index_mutex);
        return index_key1.equal_range(key); 
    }

    const std::pair<typename IndexKey2::iterator,typename IndexKey2::iterator> find_by_key2(const Key2 &  key)
    {
        std::lock_guard<Mutex> lock(index_mutex);
        return index_key2.equal_range(key); 
    } 
 

    std::multimap<Key1, Record *  >  index_key1 ; 
    std::multimap<Key2, Record *  >  index_key2 ; 
    std::map<Key, Record * > data_map ; 
    std::stack<Transaction > trans_stack = {}; 

    bool   has_transaction = false ;
	Mutex data_mutex;
    Mutex index_mutex;
}; 




