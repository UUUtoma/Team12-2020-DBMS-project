#include"pm_ehash.h"
#include"data_page.h"
#include <cmath>
/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {

}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {

}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
    return 1;
}

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
    return 1;
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    return 1;
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist) 
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
    return 1;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
    uint64_t hash_value = key / metadata->catalog_size;
    return hash_value;
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
    uint64_t hash_value = hashFunc(key);
    pm_bucket* bu = pmAddr2vAddr.find(catalog.buckets_pm_address[hash_value])->second;
    kv* free_slot = getFreeKvSlot(bu);
    if(free_slot == NULL) splitBucket(hash_value);
    return pmAddr2vAddr.find(catalog.buckets_pm_address[hash_value])->second;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
    kv* freekv = NULL;
    for(int i = 0; i < BUCKET_SLOT_NUM / 8 + 1; i++){
        for(int j = 0; j < 8; j++){
            if((((bucket->bitmap[i]) & (1 << j)) >> j) == 0){
                freekv = &(bucket->slot[j + i * 8]);
                return freekv;
            }
        }
    }
    return freekv;
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
    pm_bucket* bu = pmAddr2vAddr.find(catalog.buckets_pm_address[bucket_id])->second;
    bu->local_depth++;
    if(bu->local_depth > metadata->global_depth) extendCatalog();
    pm_bucket* new_bu;
    new_bu->local_depth = bu->local_depth;

    pm_address new_pm_addr;
    if(catalog.buckets_pm_address[metadata->catalog_size].offset == 120){
        new_pm_addr.offset = 0;
        new_pm_addr.fileId = metadata->max_file_id;
    }
    else{
        new_pm_addr.offset = catalog.buckets_pm_address[metadata->catalog_size].offset + 8;
        new_pm_addr.fileId = metadata->max_file_id - 1;
    }  
    
    for(int i = 0; i < metadata->catalog_size; i += pow(2,metadata->global_depth - bu->local_depth + 1)){
        int j = i + pow(2,  metadata->global_depth - bu->local_depth + 1);
        if(bucket_id >= i && bucket_id < j){
            int m = 0;
            for(int n = 0; n < BUCKET_SLOT_NUM; n++){
             if(hashFunc(bu->slot[n].key) >= i + pow(2,  metadata->global_depth - bu->local_depth)){
                    bu->bitmap[n / 8] ^= (bu->bitmap[n / 8] & (1 << (n / 8))) ^ (0 << (n / 8));
                    new_bu->slot[m] = bu->slot[n];
                    new_bu->bitmap[m / 8] ^= (new_bu->bitmap[m / 8] & (1 << (m / 8))) ^ (1 << (m / 8)); 
                }
            }

            pm_bucket** free_slot = (pm_bucket**)getFreeSlot(new_pm_addr);
            free_slot = &new_bu;

            for(int k = i + pow(2,metadata->global_depth - bu->local_depth); k < j; k++){
                catalog.buckets_pm_address[k] = new_pm_addr;
                catalog.buckets_virtual_address[k] = *free_slot;
            }
            break;
        }
    }
    pmAddr2vAddr.insert(std::make_pair(new_pm_addr,bu));
    vAddr2pmAddr.insert(std::make_pair(bu,new_pm_addr));
}

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
    pm_address addr = catalog.buckets_pm_address[bucket_id];
    pm_bucket* vir_addr = pmAddr2vAddr.find(addr)->second;
    pmAddr2vAddr.erase(addr);
    vAddr2pmAddr.erase(vir_addr);
    freePageSlot(catalog.buckets_virtual_address[bucket_id]);
    free_list.push(catalog.buckets_virtual_address[bucket_id]);

    pm_bucket* origin_vir_addr;
    pm_address origin_addr;
    for(int i = 0; i < metadata->catalog_size; i += pow(2,metadata->global_depth - vir_addr->local_depth + 1)){
        int j = i + pow(2,  metadata->global_depth - vir_addr->local_depth + 1);
        if(bucket_id >= i && bucket_id < j){
            if(bucket_id < i + pow(2,  metadata->global_depth - vir_addr->local_depth)){
                origin_addr = catalog.buckets_pm_address[j - 1];
                origin_vir_addr = pmAddr2vAddr.find(origin_addr)->second;
                for(int k = i + pow(2,metadata->global_depth - vir_addr->local_depth); k < j; k++){
                    catalog.buckets_pm_address[k] = origin_addr;
                    catalog.buckets_virtual_address[k] = origin_vir_addr; 
                }
            }
            else{
                origin_addr = catalog.buckets_pm_address[i];
                origin_vir_addr = pmAddr2vAddr.find(origin_addr)->second;
                for(int k = i ; k < i + pow(2,metadata->global_depth - vir_addr->local_depth); k++){
                    catalog.buckets_pm_address[k] = origin_addr;
                    catalog.buckets_virtual_address[k] = origin_vir_addr; 
                }
            }
            break;
        }
    }
    origin_vir_addr->local_depth--;
}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
    ehash_catalog* new_catalog;
    for(int i = 0; i < metadata->catalog_size; i++){
        new_catalog->buckets_pm_address[i * 2] = catalog.buckets_pm_address[i];
        new_catalog->buckets_pm_address[i * 2 + 1] = catalog.buckets_pm_address[i];
        new_catalog->buckets_virtual_address[i * 2] = catalog.buckets_virtual_address[i];
        new_catalog->buckets_virtual_address[i * 2 + 1] = catalog.buckets_virtual_address[i];
    }
    delete(catalog.buckets_pm_address);
    delete(catalog.buckets_virtual_address);
    catalog = *new_catalog;
    delete(new_catalog);
    metadata->global_depth++;
    metadata->catalog_size *= 2;
}

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {

}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {

}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {

}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置 
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {

}

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {

}