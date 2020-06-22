#include<sstream>
#include<vector>
#include<utility>
#include<libpmem.h>
#include"pm_ehash.h"
#include"data_page.h"

using std::vector;
using std::stringstream;
using std::make_pair;

// 数据页表的相关操作实现都放在这个源文件下，如PmEHash申请新的数据页和删除数据页的底层实现

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
	if (free_list.empty())
		allocNewPage();
	pm_bucket* free_slot = free_list.front();
	free_list.pop();
	new_address = (vAddr2pmAddr.find(free_slot))->second;
	return free_slot;
}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
	// new page
	size_t map_len;
	int is_pmem;
	stringstream ss;
	ss << "data/" << metadata->max_file_id;
	data_page* new_page = (data_page*)pmem_map_file(ss.str().c_str(), sizeof(data_page), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
	pmem_persist(new_page, map_len);
	pmem_unmap(new_page, map_len);
	// 更新free_list
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
		free_list.push(&(new_page->slots[i]));
	}
	// 更新metadata
	metadata->max_file_id++;
}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
	size_t map_len;
	int is_pmem;
	stringstream path_ss;
	// 读取metadata文件中的数据并内存映射
	path_ss << PM_EHASH_DIRECTORY;
	path_ss << "/" << META_NAME;
	metadata = (ehash_metadata*)pmem_map_file(path_ss.str().c_str(), sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
	// 读取catalog文件中的数据并内存映射
	path_ss.clear();
	path_ss.str("");
	path_ss << PM_EHASH_DIRECTORY;
	path_ss << "/" << CATALOG_NAME;
	catalog.buckets_pm_address = (pm_address*)pmem_map_file(path_ss.str().c_str(), sizeof(ehash_catalog), PMEM_FILE_CREATE, 0666, &map_len, &is_pmem);
	//catalog = (ehash_catalog*)pmem_map_file(path_ss.str().c_str(), sizeof(ehash_catalog), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
 	// 读取所有数据页文件并内存映射
 	// 设置可扩展哈希的桶的虚拟地址指针
 	mapAllPage();
 	// 初始化所有其他可扩展哈希的内存数据

}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置 
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {
	uint64_t catalog_size = metadata->catalog_size;
	pm_address* b_pm_addr = catalog.buckets_pm_address;
	pm_bucket** b_v_addr = catalog.buckets_virtual_address;
	pages_vitual_addr.resize(metadata->max_file_id); // virtual address of all data_page
	bool page_has_map[metadata->max_file_id];
 	int i;
 	for (i = 0; i < catalog_size / DATA_PAGE_SLOT_NUM + 1; ++i) {
 		page_has_map[i] = false;
 	}

 	for (i = 0; i < catalog_size; ++i) {
    	uint32_t fid = b_pm_addr[i].fileId;
    	if (!page_has_map[fid]) {
			size_t map_len;
			int is_pmem;
			stringstream path_ss;
			path_ss << PM_EHASH_DIRECTORY;
			path_ss << "/" << fid;
    		pages_vitual_addr[fid] = (data_page*)pmem_map_file(path_ss.str().c_str(), sizeof(data_page), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
    		page_has_map[fid] = true;
    		// 初始化free_list
    		int j;
	    	for (j = 0; j < pages_vitual_addr[fid]->bitmap.size(); ++j) {
	    		if (pages_vitual_addr[fid]->bitmap[j] == 0) free_list.push(&(pages_vitual_addr[fid]->slots[j]));
	    	}
    	}
    	pm_bucket* v_addr = &(pages_vitual_addr[fid]->slots[b_pm_addr[i].offset / sizeof(pm_bucket)]);
		// 初始化 vAddr2pmAddr
    	// 初始化 pmAddr2vAddr
    	vAddr2pmAddr.insert(make_pair(v_addr, b_pm_addr[i]));
    	pmAddr2vAddr.insert(make_pair(b_pm_addr[i], v_addr));
 	}	
}

/**
 * @description: 将空bucket对应的data_page的槽位清空
 * @param pm_bucket* bucket: 空bucket的虚拟地址
 * @return: NULL
 */
void PmEHash::freePageSlot(pm_bucket* bucket) {
	pm_address pm_addr = vAddr2pmAddr.find(bucket)->second;
	int slot_index = pm_addr.offset / sizeof(pm_bucket);
	pages_vitual_addr[pm_addr.fileId]->bitmap[slot_index] = 0;
}