#include <libpmem.h>
#include <fstream>
#include <string>

#include"pm_ehash.h"
#include"data_page.h"
using std::make_pair;
using std::string;
/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {

    string metadata_path = PM_EHASH_DIRECTORY + '/' + META_NAME;
    string catalog_path = PM_EHASH_DIRECTORY + '/' + CATALOG_NAME;
	std::ifstream metadata_file(metadata_path, std::ios::in);
	std::ifstream catalog_file(catalog_path, std::ios::in);
	if (metadata_file.is_open() && catalog_file.is_open()) 
		recover();
	else {
		int is_pmem;
		size_t mapped_len;
		metadata = (ehash_metadata*)pmem_map_file(metadata_path.c_str(), sizeof(ehash_metadata), PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
		metadata->max_file_id = 1;
		metadata->catalog_size = 0;
		metadata->global_depth = 0;		
		
		catalog->buckets_pm_address = (pm_address*)pmem_map_file(catalog_path.c_str(), sizeof(catalog), PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
		// (catalog->buckets_pm_address)->fileId = 1;
		// (catalog->buckets_pm_address)->offset = 0;
		//buckets_virtual_address ??	
	}
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {
	int is_pmem = pmem_is_pmem(metadata, sizeof(ehash_metadata));
	if (is_pmem)
		pmem_persist(metadata, sizeof(ehash_metadata));
	else
		pmem_msync(metadata, sizeof(ehash_metadata));
	pmem_unmap(metadata, sizeof(ehash_metadata));

	is_pmem = pmem_is_pmem(catalog->buckets_pm_address, sizeof(catalog->buckets_pm_address) * metadata->catalog_size);
	if (is_pmem)
		pmem_persist(catalog->buckets_pm_address, sizeof(catalog->buckets_pm_address) * metadata->catalog_size);
	else
		pmem_msync(catalog->buckets_pm_address, sizeof(catalog->buckets_pm_address) * metadata->catalog_size);
	pmem_unmap(catalog->buckets_pm_address, sizeof(catalog->buckets_pm_address) * metadata->catalog_size);
}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
	uint64_t temp_value;
	if (search(new_kv_pair.key, temp_value) == 0)	return -1;
	pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
	kv* freePlace = getFreeKvSlot(bucket);
	*freePlace = new_kv_pair;
	int index = freePlace - bucket->slot;
	bucket->bitmap[index / 8] |= (1 << (BUCKET_SLOT_NUM - index - 1));
	//persist(freePlace);
	return 0;
}

/** 
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
	uint64_t bucket_id = hashFunc(key);
	pm_bucket** virtual_address = catalog->buckets_virtual_address;
	pm_bucket bucket = *(virtual_address[bucket_id]);

	for (int i = 0; i < BUCKET_SLOT_NUM; ++i) {
		kv* temp = bucket.slot + i;
		if (temp == NULL)	break;
		if ((*temp).key == key) {
			bucket.bitmap[i / 8] &= ~(1 << (BUCKET_SLOT_NUM - i - 1));
			if (i == 0)
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
	uint64_t bucket_id = hashFunc(kv_pair.key);
	pm_bucket** virtual_address = catalog->buckets_virtual_address;
	pm_bucket bucket = *(virtual_address[bucket_id]);

	for (int i = 0; i < BUCKET_SLOT_NUM; ++i) {
		kv* temp = bucket.slot + i;
		if (temp == NULL)	break;
		if ((*temp).key == kv_pair.key) {
			(bucket.slot[i]).value = kv_pair.value;
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
	uint64_t bucket_id = hashFunc(key);
	pm_bucket** virtual_address = catalog->buckets_virtual_address;
	pm_bucket bucket = *(virtual_address[bucket_id]);

	for (int i = 0; i < BUCKET_SLOT_NUM; ++i) {
		kv* temp = bucket.slot + i;
		if (temp == NULL)	break;
		if ((*temp).key == key) {
			return_val = (*temp).value;
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
    uint64_t hash_value = key / metadata->catalog_size;
    return hash_value;
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 待插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
    uint64_t hash_value = hashFunc(key);
    pm_bucket* bu = pmAddr2vAddr.find(catalog->buckets_pm_address[hash_value])->second;
    //判断桶中是否有空闲slot，如果没有，分裂桶并且重新计算桶号
    kv* free_slot = getFreeKvSlot(bu);
    if(free_slot == NULL){
        splitBucket(hash_value);
        hash_value = hashFunc(key);
    } 
    return pmAddr2vAddr.find(catalog->buckets_pm_address[hash_value])->second;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
    kv* freekv = NULL;
    //遍历位图数组，对每个uint8_t类型数值遍历查看每一位是否为0（表示空闲），找到相应空闲位置并返回
    for(int i = 0; i < BUCKET_SLOT_NUM / 8 + 1; i++){
        for(int j = 0; j < 8; j++){
            if((((bucket->bitmap[i]) & (1 << j)) >> j) == 0){
                freekv = &(bucket->slot[j + i * 8]);
                return freekv;
            }
        }
    }
    //若没有空闲位置，返回空指针
    return freekv;
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
//产生新的桶
    pm_bucket* bu = pmAddr2vAddr.find(catalog->buckets_pm_address[bucket_id])->second;
    //被分裂的桶local depth加一
    bu->local_depth++;
    //当global depth小于local depth的时候，需要倍增目录
    if(bu->local_depth > metadata->global_depth) extendCatalog();
    //新桶的local depth与被分裂的桶相同
    pm_bucket* new_bu;
    new_bu->local_depth = bu->local_depth;

//产生对应新桶的pm_address
    pm_address new_pm_addr;
    //判断当前数据页是否满，由于一个数据页有16个slot，因此若catalog中最后一个pm_address的偏移量为120，则当前页为满。
    //fileId为下一个数据页，偏移量为0；否则为当前数据页，偏移量递增。
    if(catalog->buckets_pm_address[metadata->catalog_size].offset == 120){
        new_pm_addr.offset = 0;
        new_pm_addr.fileId = metadata->max_file_id;
    }
    else{
        new_pm_addr.offset = catalog->buckets_pm_address[metadata->catalog_size].offset + 8;
        new_pm_addr.fileId = metadata->max_file_id - 1;
    }  

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

            //设置数据页空闲slot
            pm_bucket** free_slot = (pm_bucket**)getFreeSlot(new_pm_addr);
            free_slot = &new_bu;

            //桶号在[(i + j) / 2, j)之间的catalog设置新的内容
            for(int k = i + pow(2,metadata->global_depth - bu->local_depth); k < j; k++){
                catalog->buckets_pm_address[k] = new_pm_addr;
                catalog->buckets_virtual_address[k] = *free_slot;
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

    pm_address addr = catalog->buckets_pm_address[bucket_id];
    pm_bucket* vir_addr = pmAddr2vAddr.find(addr)->second;
    //删除map中需要合并的桶的实地址、虚地址关系
    pmAddr2vAddr.erase(addr);
    vAddr2pmAddr.erase(vir_addr);
    //释放桶的地址空间，设置数据页位图，将空出来的slot加入free_list
    delete(catalog->buckets_virtual_address[bucket_id]);
    freePageSlot(catalog->buckets_virtual_address[bucket_id]);
    free_list.push(catalog->buckets_virtual_address[bucket_id]);

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
                origin_addr = catalog->buckets_pm_address[j - 1];
                origin_vir_addr = pmAddr2vAddr.find(origin_addr)->second;
                //循环更改上半区间地址
                for(int k = i + pow(2,metadata->global_depth - vir_addr->local_depth); k < j; k++){
                    catalog->buckets_pm_address[k] = origin_addr;
                    catalog->buckets_virtual_address[k] = origin_vir_addr; 
                }
            }
            //如果bucket_id在下半区间，则这个区间的实地址、虚地址更改为上半区间对应桶的地址
            else{
                //随意取上半区间的一组地址
                origin_addr = catalog->buckets_pm_address[i];
                origin_vir_addr = pmAddr2vAddr.find(origin_addr)->second;
                //循环更改下半区间地址
                for(int k = i ; k < i + pow(2,metadata->global_depth - vir_addr->local_depth); k++){
                    catalog->buckets_pm_address[k] = origin_addr;
                    catalog->buckets_virtual_address[k] = origin_vir_addr; 
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
    ehash_catalog* new_catalog;
    //循环复制旧目录中的值，桶号i * 2和i * 2 + 1对应的地址相同
    for(int i = 0; i < metadata->catalog_size; i++){
        new_catalog->buckets_pm_address[i * 2] = catalog->buckets_pm_address[i];
        new_catalog->buckets_pm_address[i * 2 + 1] = catalog->buckets_pm_address[i];
        new_catalog->buckets_virtual_address[i * 2] = catalog->buckets_virtual_address[i];
        new_catalog->buckets_virtual_address[i * 2 + 1] = catalog->buckets_virtual_address[i];
    }
    //回收旧的目录文件的空间
    delete(catalog->buckets_pm_address);
    delete(catalog->buckets_virtual_address);
    //将新的目录文件赋给catalog
    catalog = new_catalog;
    //回收作为中转的新目录文件
    delete(new_catalog);
    //目录倍增后global depth需要加一，catalog size要倍增
    metadata->global_depth++;
    metadata->catalog_size *= 2;
}


/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
	char command_line[256];
	sprintf(command_line, "rm -f %s/*", PM_EHASH_DIRECTORY);
	system(command_line);
	return;
}
