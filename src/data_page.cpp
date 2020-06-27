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
	// 若没有可用数据页，申请新数据页
	if (free_list.empty())
		allocNewPage();
	// 获得可用数据页槽位
	pm_bucket* free_slot = free_list.front();
	free_list.pop();
	new_address = (vAddr2pmAddr.find(free_slot))->second;
	// 被使用的数据页槽位对应位图置1
	int slot_index = new_address.offset / sizeof(pm_bucket);
	pages_virtual_addr[new_address.fileId]->bitmap[slot_index] = 1;
	// 页文件数据持久化
	pmem_persist(pages_virtual_addr[new_address.fileId], sizeof(data_page));

	return free_slot;
}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
	// 创建新的数据页文件
	size_t map_len;
	int is_pmem;
	stringstream ss;
	ss << PM_EHASH_DIRECTORY << "/" << metadata->max_file_id;
	data_page* new_page = (data_page*)pmem_map_file(ss.str().c_str(), sizeof(data_page), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
	// 更新 pages_virtual_addr
	pages_virtual_addr.push_back(new_page);
	// 初始化新空桶, 更新 free_list, vAddr2pmAddr, pmAddr2vAddr
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
		pm_bucket* v_addr = &(pages_virtual_addr.back()->slots[i]);
		pm_address pm_addr = {(uint32_t)metadata->max_file_id, (uint32_t)(i * sizeof(pm_bucket))};

		// Initialize new bucket
		v_addr->local_depth = 1;
		for (int j = 0; j < BUCKET_SLOT_NUM; ++j) {
			v_addr->bitmap[j / 8] ^= (v_addr->bitmap[j / 8] & (1 << (j % 8))) ^ (0 << (j % 8));
		}

		free_list.push(&(new_page->slots[i]));
		vAddr2pmAddr.insert(make_pair(v_addr, pm_addr));
    	pmAddr2vAddr.insert(make_pair(pm_addr, v_addr));
	}
	// 页文件数据持久化
	pmem_persist(new_page, map_len);
	// 若此时unmap，页虚地址失效，应该在析构函数unmap
	// pmem_unmap(new_page, map_len);

	// 更新 metadata
	metadata->max_file_id++;
	// metadata 数据持久化
	pmem_persist(metadata, sizeof(ehash_metadata));

}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
	size_t map_len;
	int is_pmem;
	//获得metadata文件和catalog文件的路径
	char metadata_path[256], catalog_path[256];
	sprintf(metadata_path, "%s/%s", PM_EHASH_DIRECTORY, META_NAME);
	sprintf(catalog_path, "%s/%s", PM_EHASH_DIRECTORY, CATALOG_NAME);
	// 读取metadata文件中的数据并内存映射
	metadata = (ehash_metadata*)pmem_map_file(metadata_path, sizeof(ehash_metadata), 0, 0666, &map_len, &is_pmem);
	// 读取catalog文件中的数据并内存映射
	catalog.buckets_pm_address = (pm_address*)pmem_map_file(catalog_path, sizeof(ehash_catalog), 0, 0666, &map_len, &is_pmem);
	//catalog = (ehash_catalog*)pmem_map_file(path_ss.str().c_str(), sizeof(ehash_catalog), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
 	// 读取所有数据页文件并内存映射，设置地址映射关系
 	// 设置可扩展哈希的桶的虚拟地址指针
 	mapAllPage();
	return;
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
	pages_virtual_addr.resize(metadata->max_file_id); // virtual address of all data_page
	bool page_has_map[metadata->max_file_id]; // 记录已被内存映射的数据页
 	int i;
 	for (i = 0; i < catalog_size / DATA_PAGE_SLOT_NUM + 1; ++i) {
 		page_has_map[i] = false;
 	}
	// 遍历catalog中的pm_address，依次映射每个bucket的虚拟地址
 	for (i = 0; i < catalog_size; ++i) {
    	uint32_t fid = b_pm_addr[i].fileId;
		// 若还没有映射当前pm_address对应bucket的page，内存映射对应page并且将page内的free_slot压入free_list
    	if (!page_has_map[fid]) {
			size_t map_len;
			int is_pmem;
			stringstream path_ss;
			path_ss << PM_EHASH_DIRECTORY;
			path_ss << "/" << fid;
    		pages_virtual_addr[fid] = (data_page*)pmem_map_file(path_ss.str().c_str(), sizeof(data_page), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
    		page_has_map[fid] = true;
    		// 初始化free_list，将当前page中的空槽位添加到free_list
    		int j;
	    	for (j = 0; j < pages_virtual_addr[fid]->bitmap.size(); ++j) {
	    		if (pages_virtual_addr[fid]->bitmap[j] == 0) free_list.push(&(pages_virtual_addr[fid]->slots[j]));
	    	}
    	}
		// 计算当前pm_address对应bucket的虚拟地址（使用page的虚拟地址，计算当前bucket在对应page的slots数组index）
    	pm_bucket* v_addr = &(pages_virtual_addr[fid]->slots[b_pm_addr[i].offset / sizeof(pm_bucket)]);
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
	// 计算页号与槽号
	pm_address pm_addr = vAddr2pmAddr.find(bucket)->second;
	int slot_index = pm_addr.offset / sizeof(pm_bucket);
	// 对应槽位图置0
	pages_virtual_addr[pm_addr.fileId]->bitmap[slot_index] = 0;
	// 页文件数据持久化
	pmem_persist(pages_virtual_addr[pm_addr.fileId], sizeof(data_page));
}


/**
 * @description: 首次创建数据页，与allocNewPage不同：默认的初始桶对应的slot不用压入free_list
 * @param int: 
 * @return: NULL
 */
void PmEHash::firstNewPage(int default_bucket_count) {
	// new page
	size_t map_len;
	int is_pmem;
	stringstream ss;
	ss << PM_EHASH_DIRECTORY << "/" << metadata->max_file_id;
	data_page* new_page = (data_page*)pmem_map_file(ss.str().c_str(), sizeof(data_page), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
	// 更新 pages_virtual_addr
	pages_virtual_addr.resize(2);
	pages_virtual_addr[1] = new_page;
	// 初始化新空桶, 更新 free_list, vAddr2pmAddr, pmAddr2vAddr
	for (int i = 0; i < DATA_PAGE_SLOT_NUM; ++i) {
		pm_bucket* v_addr = &(pages_virtual_addr.back()->slots[i]);
		pm_address pm_addr = {(uint32_t)metadata->max_file_id, (uint32_t)(i * sizeof(pm_bucket))};

		// Initialize
		v_addr->local_depth = 1;
		for (int j = 0; j < BUCKET_SLOT_NUM; ++j) {
			v_addr->bitmap[j / 8] ^= (v_addr->bitmap[j / 8] & (1 << (j % 8))) ^ (0 << (j % 8));
		}
		// 如果当前桶是默认桶，则对应数据页的槽位不添加到free_list
		if (i >= default_bucket_count) {
			free_list.push(&(new_page->slots[i]));
		}
		vAddr2pmAddr.insert(make_pair(v_addr, pm_addr));
    	pmAddr2vAddr.insert(make_pair(pm_addr, v_addr));
	}
	// 页文件数据持久化
	pmem_persist(new_page, map_len);
	// 若此时unmap，页虚地址失效，应该在析构函数unmap
	// pmem_unmap(new_page, map_len);

	// 更新 metadata
	metadata->max_file_id++;
	// metadata数据持久化
	pmem_persist(metadata, sizeof(ehash_metadata));

}
