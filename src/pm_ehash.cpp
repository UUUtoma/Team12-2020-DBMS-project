#include <libpmem.h>
#include <fstream>
#include <cstring>
#include <string>

#include"pm_ehash.h"
#include"data_page.h"
using namespace std;

/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
    //获得metadata文件和catalog文件的路径
    char metadata_path[256], catalog_path[256];
    sprintf(metadata_path, "%s/%s", PM_EHASH_DIRECTORY, META_NAME);
    sprintf(catalog_path, "%s/%s", PM_EHASH_DIRECTORY, CATALOG_NAME);

    //根据路径查找metadata文件和catalog文件
    int is_pmem;
    size_t mapped_len;
    metadata = (ehash_metadata*)pmem_map_file(metadata_path, sizeof(ehash_metadata), 0, 0, &mapped_len, &is_pmem);
    catalog.buckets_pm_address = (pm_address*)pmem_map_file(catalog_path, sizeof(ehash_catalog), 0, 0, &mapped_len, &is_pmem);
    
    //如果metadata文件或catalog文件不存在
    if (metadata == nullptr || catalog.buckets_pm_address == nullptr){

        //创建metadata文件并初始化记录的数据
        metadata = (ehash_metadata*)pmem_map_file(metadata_path, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, &mapped_len, &is_pmem);
        metadata->max_file_id = 1;
        metadata->catalog_size = DEFAULT_CATALOG_SIZE;
        metadata->global_depth = 4;

        //分配初始数据页以存储数据
        firstNewPage(DEFAULT_CATALOG_SIZE);
        
        //创建catalog文件并初始化所有文件地址和虚拟地址
        catalog.buckets_pm_address = (pm_address*)pmem_map_file(catalog_path, sizeof(pm_address)*DEFAULT_CATALOG_SIZE, PMEM_FILE_CREATE, 0777, &mapped_len, &is_pmem);
        catalog.buckets_virtual_address = new pm_bucket*[DEFAULT_CATALOG_SIZE];
        for (int i = 0; i < DEFAULT_CATALOG_SIZE; ++i){
            catalog.buckets_pm_address[i].fileId = 1;
            catalog.buckets_pm_address[i].offset = i * sizeof(pm_bucket);
            catalog.buckets_virtual_address[i] = pmAddr2vAddr[catalog.buckets_pm_address[i]];
        }
    }
    
    //如果metadata文件和catalog文件存在
    else 
        //恢复原可扩展哈希的状态
        recover();
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {
    //判断metadata文件对应的内存是否为持久性内存
    int is_pmem = pmem_is_pmem(metadata, sizeof(ehash_metadata));

    //将metadata文件的所有更改持久存储在持久性内存中
    if (is_pmem)
        pmem_persist(metadata, sizeof(ehash_metadata));
    else
        pmem_msync(metadata, sizeof(ehash_metadata));
    
    //取消metadata文件到持久性内存的映射
    pmem_unmap(metadata, sizeof(ehash_metadata));

    //判断catalog文件对应的内存是否为持久性内存
    is_pmem = pmem_is_pmem(catalog.buckets_pm_address, sizeof(catalog.buckets_pm_address) * metadata->catalog_size);
    
    //将catalog文件的所有更改持久存储在持久性内存中
    if (is_pmem)
        pmem_persist(catalog.buckets_pm_address, sizeof(catalog.buckets_pm_address) * metadata->catalog_size);
    else
        pmem_msync(catalog.buckets_pm_address, sizeof(catalog.buckets_pm_address) * metadata->catalog_size);
    
    //取消catalog文件到持久性内存的映射
    pmem_unmap(catalog.buckets_pm_address, sizeof(catalog.buckets_pm_address) * metadata->catalog_size);

    // 将数据页文件更改持久存储在持久性内存中，取消数据页文件到持久性内存的映射
    for (vector<data_page*>::iterator i = pages_virtual_addr.begin(); i != pages_virtual_addr.end(); ++i) {
    	is_pmem = pmem_is_pmem((*i), sizeof(data_page));
    	if (is_pmem)
    		pmem_persist((*i), sizeof(data_page));
    	else
    		pmem_msync((*i), sizeof(data_page));
    	pmem_unmap((*i), sizeof(data_page));
    }
}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
    uint64_t temp_value;
    //限制条件：若待插入的键已存在则返回-1，表示插入失败
    if (search(new_kv_pair.key, temp_value) == 0)   return -1;
    //获得可插入的桶
    pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
    //获得可插入的插槽
    kv* freePlace = getFreeKvSlot(bucket);  
    //将数据写入插槽中
    *freePlace = new_kv_pair;
    //计算插入位置对应位图的索引
    int index = freePlace - bucket->slot;
    //设置位图上相应位置为1
    bucket->bitmap[index / 8] |= (1 << (index % 8));
    //操作成功，返回0
    return 0;
}

/** 
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
    uint64_t temp_value;
    //限制条件：若待删除的键值对不存在则返回-1，表示删除失败
    if (search(key, temp_value) == -1) return -1;
    //根据哈希函数计算得到待删除的数据对应的桶的桶号
    uint64_t bucket_id = hashFunc(key);
    //得到目录文件记录的桶虚拟地址的首地址
    pm_bucket** virtual_address = catalog.buckets_virtual_address;
    //得到待删除的数据对应的桶的虚拟地址
    pm_bucket* bucket = virtual_address[bucket_id];

    //遍历桶中的非空插槽，查找待删除的键值对
    uint8_t temp;
    for (int i = 0; i < BUCKET_SLOT_NUM; ++i){
        //获得位置i的位图
        temp = bucket->bitmap[i / 8];
        //如果位图中位置i的值为1，即查找到了待删除的键值对
        if ((temp >> (i % 8)) & 1){
            //将位图相应位置的值置0，表示数据删除（暂时没有真正删除数据）
            bucket->bitmap[i / 8] &= ~(1 << (i % 8));
            //遍历检验位图，若都为0，则为空桶，执行合并桶的操作
            int k, j;
            for(k = 0; k < BUCKET_SLOT_NUM / 8 + 1; k++){
                for(j = 0; j < 8; j++){
                    if((((bucket->bitmap[k]) & (1 << j)) >> j) == 1)
                        break;
                }
            }
            if(k * 8 - j == BUCKET_SLOT_NUM) 
                //调用函数mergeBucket合并指定桶
                mergeBucket(bucket_id);
            return 0;
        }
    }
    return -1;
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    //根据哈希函数计算得到待更新的数据对应的桶的桶号
    uint64_t bucket_id = hashFunc(kv_pair.key);
    //得到目录文件记录的桶虚拟地址的首地址
    pm_bucket** virtual_address = catalog.buckets_virtual_address;
    //得到待更新的数据对应的桶的虚拟地址
    pm_bucket* bucket = virtual_address[bucket_id];

    //遍历桶中的非空插槽，查找待更新的键值对
    uint8_t temp;
    for (int i = 0; i < BUCKET_SLOT_NUM; ++i){
        //获得当前位置i的位图
        temp = bucket->bitmap[i / 8];
        //如果位图中位置i的值为1，即查找到了待更新的键值对
        if ((temp >> (i % 8)) & 1){
            //更新待更新的键值对
            (bucket->slot[i]).value = kv_pair.value;
            return 0;
        }
    }
    return -1;
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist) 
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
    //根据哈希函数计算得到待查找的数据对应的桶的桶号
    uint64_t bucket_id = hashFunc(key);
    //在文件地址和虚拟地址的对应表中获得待查找的数据对应的桶
    pm_bucket* bucket = pmAddr2vAddr.find(catalog.buckets_pm_address[bucket_id])->second;
    //如果找不到对应的桶，即数据不存在，返回-1，表示查找失败
    if (bucket == nullptr)  return -1;

    //遍历桶中的非空插槽，查找待查找的键值对
    uint8_t temp;
    for (int i = 0; i < BUCKET_SLOT_NUM; ++i){
        //获得当前位置i的位图
        temp = bucket->bitmap[i / 8];
        //如果位图中位置i的值为1，即查找到了待查找的键值对
        if ((temp >> (i % 8)) & 1){
            //将待查找的键值对的值赋值给引用类型的参数return_val进行返回
            return_val = (bucket->slot[i]).value;
            return 0;
        }
    }
    return -1;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
    //直接取模求桶号
    uint64_t hash_value = key % (metadata->catalog_size);
    return hash_value;
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 待插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
    uint64_t hash_value = hashFunc(key);
    pm_bucket* bu = pmAddr2vAddr.find(catalog.buckets_pm_address[hash_value])->second;
    //判断桶中是否有空闲slot，如果没有，分裂桶并且重新计算桶号
    kv* free_slot = getFreeKvSlot(bu);
    if(free_slot == NULL){
        splitBucket(hash_value);
        hash_value = hashFunc(key);
    } 
    return pmAddr2vAddr.find(catalog.buckets_pm_address[hash_value])->second;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
    uint8_t temp;
    for (int i = 0; i < BUCKET_SLOT_NUM; ++i){
        temp = bucket->bitmap[i / 8];
        //如果空闲位置存在
        if (~((temp >> (i % 8)) & 1)){
            return &(bucket->slot[i]);
        }
    }
    return NULL;

}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {

//设置数据页空闲slot
    pm_address new_pm_addr;
    pm_bucket* new_bu = (pm_bucket*)getFreeSlot(new_pm_addr);

//产生新的桶
    //被分裂的桶local depth加一
    pm_bucket* bu = pmAddr2vAddr.find(catalog.buckets_pm_address[bucket_id])->second;
    bu->local_depth++;
    //当global depth小于local depth的时候，需要倍增目录
    if(bu->local_depth > metadata->global_depth) extendCatalog();
    //新桶的local depth与被分裂的桶相同
    new_bu->local_depth = bu->local_depth;

//将桶中满足要求的数据放入新的桶中，为新的桶设置catalog。其中序号为桶所在区间的后半区间的catalog指向新桶。
    //使用循环，寻找bucket_id所在区间
    for(int i = 0; i < metadata->catalog_size; i += pow(2,metadata->global_depth - bu->local_depth + 1)){
        int j = i + pow(2,  metadata->global_depth - bu->local_depth + 1);
        if(bucket_id >= i && bucket_id < j){
            //如果bucket_id在当前区间[i,j)中，遍历分裂桶的slot
            //如果位图为1且桶号在[(i + j) / 2, j)中，则放入新的桶中
            int m = 0;
            for(int n = 0; n < BUCKET_SLOT_NUM; n++){
             if(((bu->bitmap[n / 8] & (1 << (n % 8))) >> (n % 8)) == 1 
                && (bu->slot[n].key) >= i + pow(2,  metadata->global_depth - bu->local_depth)){
                    //设置当前位图和新桶位图
                    bu->bitmap[n / 8] ^= (bu->bitmap[n / 8] & (1 << (n % 8))) ^ (0 << (n % 8));
                    new_bu->slot[m] = bu->slot[n];
                    new_bu->bitmap[m / 8] ^= (new_bu->bitmap[m / 8] & (1 << (m % 8))) ^ (1 << (m % 8)); 
                }
            }

            //桶号在[(i + j) / 2, j)之间的catalog设置新的内容
            for(int k = i + pow(2,metadata->global_depth - bu->local_depth); k < j; k++){
                catalog.buckets_pm_address[k] = new_pm_addr;
                catalog.buckets_virtual_address[k] = new_bu;
            }
            break;
        }
    }
    //在map中插入新的实地址、虚地址关系，便于查找
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
    
    //释放桶的地址空间，设置数据页位图，将空出来的slot加入free_list

    int slot_index = addr.offset / sizeof(pm_bucket);
    pages_virtual_addr[addr.fileId]->bitmap[slot_index] = 0;
    free_list.push(catalog.buckets_virtual_address[bucket_id]);

    //删除map中需要合并的桶的实地址、虚地址关系
    pmAddr2vAddr.erase(addr);
    vAddr2pmAddr.erase(vir_addr);

    //将要与该桶合并的桶的实地址、虚地址
    pm_bucket* origin_vir_addr;
    pm_address origin_addr;
    //循环，寻找bucket_id所在区间（该桶、与该桶合并的桶涉及的区间）
    for(int i = 0; i < metadata->catalog_size; i += pow(2,metadata->global_depth - vir_addr->local_depth + 1)){
        int j = i + pow(2,  metadata->global_depth - vir_addr->local_depth + 1);
        if(bucket_id >= i && bucket_id < j){
            //如果bucket_id在[i, (i + j) / 2)中，则这个区间的实地址、虚地址更改为下半区间对应桶的地址
            if(bucket_id < i + pow(2,  metadata->global_depth - vir_addr->local_depth)){
                //随意取下半区间的一组地址，因为都相同
                origin_addr = catalog.buckets_pm_address[j - 1];
                origin_vir_addr = pmAddr2vAddr.find(origin_addr)->second;
                //循环更改上半区间地址
                for(int k = i + pow(2,metadata->global_depth - vir_addr->local_depth); k < j; k++){
                    catalog.buckets_pm_address[k] = origin_addr;
                    catalog.buckets_virtual_address[k] = origin_vir_addr; 
                }
            }
            //如果bucket_id在下半区间，则这个区间的实地址、虚地址更改为上半区间对应桶的地址
            else{
                //随意取上半区间的一组地址
                origin_addr = catalog.buckets_pm_address[i];
                origin_vir_addr = pmAddr2vAddr.find(origin_addr)->second;
                //循环更改下半区间地址
                for(int k = i ; k < i + pow(2,metadata->global_depth - vir_addr->local_depth); k++){
                    catalog.buckets_pm_address[k] = origin_addr;
                    catalog.buckets_virtual_address[k] = origin_vir_addr; 
                }
            }
            break;
        }
    }
    //最终合并的桶的local_depth需要减一
    origin_vir_addr->local_depth--;
}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
    //更改元数据
    metadata->global_depth++;
    metadata->catalog_size *= 2;

    //扩展目录的中的pm_address
    memcpy((void*)(catalog.buckets_pm_address + (metadata->catalog_size * sizeof(pm_address)) / 2), (void*)catalog.buckets_pm_address, (metadata->catalog_size * sizeof(pm_address)) / 2);

    //申请新的虚拟地址的空间
    pm_bucket** new_buckets_virtual_address = new pm_bucket* [metadata->catalog_size];
    //复制旧的虚拟地址到新的虚拟地址
    memcpy((void*)new_buckets_virtual_address, (void*)catalog.buckets_virtual_address, sizeof(pm_bucket*) * metadata->catalog_size / 2);
    memcpy((void*)(new_buckets_virtual_address + metadata->catalog_size / 2), (void*)catalog.buckets_virtual_address, sizeof(pm_bucket*) * metadata->catalog_size / 2);
    //删除旧的虚拟地址并将新的虚拟地址赋给目录
    delete[] catalog.buckets_virtual_address;
    catalog.buckets_virtual_address = new_buckets_virtual_address;

}


/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
    char command_line[256];
    //通过调用shell命令实现所有文件数据的清空
    sprintf(command_line, "rm -f %s/*", PM_EHASH_DIRECTORY);
    system(command_line);
    return;
}
