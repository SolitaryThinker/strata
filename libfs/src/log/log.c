#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "mlfs/mlfs_user.h"
#include "log/log.h"
#include "concurrency/thread.h"
#include "filesystem/fs.h"
#include "filesystem/shared.h"
#include "filesystem/slru.h"
#include "io/block_io.h"
#include "global/mem.h"
#include "global/util.h"
#include "mlfs/mlfs_interface.h"
#include "storage/storage.h"

/**
 A system call should call start_log_tx()/commit_log_tx() to mark
 its start and end. Usually start_log_tx() just increments
 the count of in-progress FS system calls and returns.

 Log appends are synchronous.
 For crash consistency, log blocks are persisted first and
 then log header is perstisted in commit_log_tx().
 Digesting happens as a unit of a log block group
 (each logheader for each group).
 n in the log header indicates # of live log blocks.
 After digesting all log blocks in a group, the digest thread
 unsets inuse bit, indicating the group  can be garbage-collected.
 Note that the log header must be less than
 a block size for crash consistency.

 On-disk format of log area
 [ log superblock | log header | log blocks | log header | log blocks ... ]
 [ log header | log blocks ] is a transaction made by a system call.

 Each logheader describes a log block group created by
 a single transaction. Different write syscalls use different log group.
 start_log_tx() start a new log group and commit_log_tx()
 serializes writing multiple log groups to log area.
 */

struct fs_log *g_fs_log;
struct fs_log *g_fs_log_secure;
struct log_superblock *g_log_sb;

// for coalescing
uint16_t *inode_version_table;
int log_rotated_during_coalescing;
int coalesce_count;
addr_t digest_blkno;

// for communication with kernel fs.
int g_sock_fd;
static struct sockaddr_un g_srv_addr, g_addr;

static void read_log_superblock(struct log_superblock *log_sb);
static void write_log_superblock(struct log_superblock *log_sb);
static void commit_log(void);
static void digest_log(void);

pthread_mutex_t *g_log_mutex_shared;

//pthread_t is unsigned long
static unsigned long digest_thread_id;
//Thread entry point
void *digest_thread(void *arg);

void init_log(int dev)
{
	int ret;
	int volatile done = 0;
	pthread_mutexattr_t attr;

	if (sizeof(struct logheader) > g_block_size_bytes) {
		printf("log header size %lu block size %lu\n",
				sizeof(struct logheader), g_block_size_bytes);
		panic("initlog: too big logheader");
	}

	g_fs_log = (struct fs_log *)mlfs_zalloc(sizeof(struct fs_log));
    g_fs_log_secure = (struct fs_log *)mlfs_zalloc(sizeof(struct fs_log));
	g_log_sb = (struct log_superblock *)mlfs_zalloc(sizeof(struct log_superblock));
	inode_version_table = (uint16_t *)mlfs_zalloc(sizeof(uint16_t) * NINODES);

	g_fs_log->log_sb_blk = g_fs_log_secure->log_sb_blk = disk_sb[dev].log_start;
	g_fs_log->size = disk_sb[dev].nlog;
	g_fs_log->dev = g_fs_log_secure->dev = dev;
	g_fs_log->nloghdr = g_fs_log_secure->nloghdr = 0;

    printf("original size of the log: %lx | original superblock number %lx\n", g_fs_log->size, g_fs_log->log_sb_blk);

	ret = pipe(g_fs_log->digest_fd);
	if (ret < 0) 
		panic("cannot create pipe for digest\n");

	read_log_superblock(g_log_sb);

    // Need to split the log into secure and unsecure sections
    // reserve 30% of the log for the coalesced digest
    g_fs_log_secure->size = g_fs_log->size;
    g_fs_log_secure->next_avail_header = (g_fs_log->size - ((30 * g_fs_log->size) / 100));
    g_fs_log_secure->next_avail = g_log_sb->secure_start_digest + 1;
    g_fs_log_secure->start_blk = g_log_sb->secure_start_digest;

    // Set the secure log size to 70% of the true size
    g_fs_log->size = g_fs_log->size - ((30 * g_fs_log->size) / 100) - 1;
	g_fs_log->log_sb = g_log_sb;

	// Assuming all logs are digested by recovery.
	g_fs_log->next_avail_header = disk_sb[dev].log_start + 1; // +1: log superblock
	g_fs_log->next_avail = g_fs_log->next_avail_header + 1;
	g_fs_log->start_blk = disk_sb[dev].log_start + 1;

	printf("start of the log %lx | end of the log %lx\n", g_fs_log->start_blk, g_fs_log->size);
    printf("start of the secure log %lx | end of the secure log %lx\n", g_fs_log_secure->start_blk, g_fs_log_secure->size);
    printf("block number of the super block %lx | size of the overall log %lx\n", disk_sb[dev].log_start, disk_sb[dev].nlog);

	g_log_sb->start_digest = g_fs_log->next_avail_header;
	g_log_sb->secure_start_digest = g_fs_log_secure->next_avail_header;

	write_log_superblock(g_log_sb);

    atomic_init(&g_log_sb->n_secure_digest, 0);
	atomic_init(&g_log_sb->n_digest, 0);

	//g_fs_log->outstanding = 0;
	g_fs_log->start_version = g_fs_log->avail_version = 0;

	pthread_spin_init(&g_fs_log->log_lock, PTHREAD_PROCESS_SHARED);
	
	// g_log_mutex_shared is shared mutex between parent and child.
	g_log_mutex_shared = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(g_log_mutex_shared, &attr);

	g_fs_log->shared_log_lock = (pthread_mutex_t *)mlfs_zalloc(sizeof(pthread_mutex_t));
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_mutex_init(g_fs_log->shared_log_lock, &attr);

	digest_thread_id = mlfs_create_thread(digest_thread, &done);

	// enable/disable statistics for log
	enable_perf_stats = 0;

	/* wait until the digest thread get started */
	while(!done);
}

void shutdown_log(void)
{
	// wait until the digest_thead finishes job.
	if (g_fs_log->digesting) {
		mlfs_info("%s", "[L] Wait finishing on-going digest\n");
		wait_on_digesting();
	}

	if (atomic_load(&g_fs_log->log_sb->n_digest)) {
		mlfs_info("%s", "[L] Digesting remaining log data\n");
		while(make_digest_request_async(100) != -EBUSY);
		m_barrier();
		wait_on_digesting();
	}
}

static void read_log_superblock(struct log_superblock *log_sb)
{
	int ret;
	struct buffer_head *bh;

	bh = bh_get_sync_IO(g_fs_log->dev, g_fs_log->log_sb_blk, 
			BH_NO_DATA_ALLOC);

	bh->b_size = sizeof(struct log_superblock);
	bh->b_data = (uint8_t *)log_sb;

	bh_submit_read_sync_IO(bh);

	bh_release(bh);

	return;
}

static void write_log_superblock(struct log_superblock *log_sb)
{
	int ret;
	struct buffer_head *bh;

	bh = bh_get_sync_IO(g_fs_log->dev, g_fs_log->log_sb_blk, 
			BH_NO_DATA_ALLOC);

	bh->b_size = sizeof(struct log_superblock);
	bh->b_data = (uint8_t *)log_sb;

	mlfs_write(bh);

	bh_release(bh);

	return;
}

static loghdr_t *read_log_header(uint16_t dev, addr_t blkno)
{
	int ret;
	struct buffer_head *bh;
	loghdr_t *hdr_data = mlfs_alloc(sizeof(struct logheader));

	bh = bh_get_sync_IO(g_fs_log->dev, blkno, BH_NO_DATA_ALLOC);
	bh->b_size = sizeof(struct logheader);
	bh->b_data = (uint8_t *)hdr_data;

	bh_submit_read_sync_IO(bh);

	return hdr_data;
}

static loghdr_meta_t *read_log_header_meta(uint16_t from_dev, addr_t hdr_addr)
{
	int ret, i;
	loghdr_t *_loghdr;
	loghdr_meta_t *loghdr_meta;

	loghdr_meta = (loghdr_meta_t *)mlfs_zalloc(sizeof(loghdr_meta_t));
	if (!loghdr_meta) 
		panic("cannot allocate logheader\n");

	INIT_LIST_HEAD(&loghdr_meta->link);

	/* optimization: instead of reading log header block to kernel's memory,
	 * buffer head points to memory address for log header block.
	 */
	_loghdr = (loghdr_t *)(g_bdev[from_dev]->map_base_addr + 
		(hdr_addr << g_block_size_shift));

	loghdr_meta->loghdr = _loghdr;
	loghdr_meta->hdr_blkno = hdr_addr;
	loghdr_meta->is_hdr_allocated = 1;


	mlfs_debug("%s", "--------------------------------\n");
	mlfs_debug("%d\n", _loghdr->n);
	mlfs_debug("next loghdr %lx\n", _loghdr->next_loghdr_blkno);
	mlfs_debug("inuse %x\n", _loghdr->inuse);

	/*
	for (i = 0; i < _loghdr->n; i++) {
		mlfs_debug("types %d blocks %lx\n", 
				_loghdr->type[i], _loghdr->blocks[i]);
	}
	*/

	return loghdr_meta;
}

inline addr_t log_alloc(uint32_t nr_blocks)
{
	int ret;
	struct fs_log *log_metadata;
	uint8_t use_secure_log = get_loghdr_meta()->secure_log;
	if (use_secure_log) {
		mlfs_assert(g_fs_log->digesting);
		log_metadata = g_fs_log_secure;
	} else {
		log_metadata = g_fs_log;
	}

	/* g_fs_log->start_blk : header
	 * g_fs_log->next_avail : tail
	 *
	 * There are two cases:
	 *
	 * <log_begin ....... log_start .......  next_avail .... log_end>
	 *	      digested data      available data       empty space
	 *
	 *	                    (ver 10)              (ver 9)
	 * <log_begin ........ next_avail .......... log_start ...... log_end>
	 *	       available data       digested data       available data
	 *
	 */
	//mlfs_assert(g_fs_log->avail_version - g_fs_log->start_version < 2);

	// Log is getting full. make asynchronous digest request.
	if (!log_metadata->digesting) {
		addr_t nr_used_blk = 0;
		if (log_metadata->avail_version == log_metadata->start_version) {
			mlfs_assert(log_metadata->next_avail >= log_metadata->start_blk);
			nr_used_blk = log_metadata->next_avail - log_metadata->start_blk; 
		} else {
			nr_used_blk = (log_metadata->size - log_metadata->start_blk);
			nr_used_blk += (log_metadata->next_avail - log_metadata->log_sb_blk);
		}

		// The 30% is ad-hoc parameter: In genernal, 30% ~ 40% shows good performance
		// in all workloads
		if (nr_used_blk > ((30 * log_metadata->size) / 100) && !use_secure_log) {
			mlfs_assert(!use_secure_log);
			// digest 90% of log.
			while(make_digest_request_async(100) != -EBUSY)
			mlfs_info("%s", "[L] log is getting full. asynchronous digest!\n");
		}
	}

	// next_avail reaches the end of log. 
	//if (log_metadata->next_avail + nr_blocks > log_metadata->log_sb_blk + log_metadata->size) {
	if (log_metadata->next_avail + nr_blocks > log_metadata->size) {
		printf("next avail %x | nr_blocks %x | size %x\n", log_metadata->next_avail, nr_blocks, log_metadata->size);
		mlfs_assert(!use_secure_log);
		log_metadata->next_avail = log_metadata->log_sb_blk + 1;

		atomic_add(&log_metadata->avail_version, 1);

		mlfs_debug("-- log tail is rotated: new start %lu\n", log_metadata->next_avail);
	}

	addr_t next_log_blk = 
		__sync_fetch_and_add(&log_metadata->next_avail, nr_blocks);

	// This has many policy questions.
	// Current implmentation is very converative.
	// Pondering the way of optimization.
retry:
	if (log_metadata->avail_version > log_metadata->start_version && !use_secure_log) {
		if (log_metadata->start_blk - log_metadata->next_avail
				< (log_metadata->size/ 5)) {
			mlfs_info("%s", "\x1B[31m [L] synchronous digest request and wait! \x1B[0m\n");
			while (make_digest_request_async(95) != -EBUSY);

			m_barrier();
			wait_on_digesting();
		}
	}

	if (log_metadata->avail_version > log_metadata->start_version && !use_secure_log) {
		if (log_metadata->next_avail > log_metadata->start_blk) 
			goto retry;
	}

	if (0) {
		int i;
		for (i = 0; i < nr_blocks; i++) {
			mlfs_info("alloc %lu\n", next_log_blk + i);
		}
	}

	return next_log_blk;
}

// allocate logheader meta and attach logheader.
static inline struct logheader_meta *loghd_alloc(struct logheader *lh)
{
	struct logheader_meta *loghdr_meta;

	loghdr_meta = (struct logheader_meta *)
		mlfs_zalloc(sizeof(struct logheader_meta));

	if (!loghdr_meta)
		panic("cannot allocate logheader_meta");

	INIT_LIST_HEAD(&loghdr_meta->link);

	loghdr_meta->loghdr = lh;

	return loghdr_meta;
}

// Write in-memory log header to disk.
// This is the true point at which the
// current transaction commits.
static void persist_log_header(struct logheader_meta *loghdr_meta,
		addr_t hdr_blkno)
{
	struct logheader *loghdr = loghdr_meta->loghdr;
	struct buffer_head *io_bh;
	int i;
	uint64_t start_tsc;

	if (enable_perf_stats)
		start_tsc = asm_rdtscp();

	io_bh = bh_get_sync_IO(g_fs_log->dev, hdr_blkno, BH_NO_DATA_ALLOC);

	//if (enable_perf_stats) {
		//g_perf_stats.bcache_search_tsc += (asm_rdtscp() - start_tsc);
		//g_perf_stats.bcache_search_nr++;
	//}
	//pthread_spin_lock(&io_bh->b_spinlock);

	mlfs_get_time(&loghdr->mtime);
	io_bh->b_data = (uint8_t *)loghdr;
	io_bh->b_size = sizeof(struct logheader);

	mlfs_write(io_bh);

	mlfs_debug("pid %u [log header] inuse %d blkno %lu next_hdr_blockno %lu\n", 
			getpid(),
			loghdr->inuse, io_bh->b_blocknr, 
			loghdr->next_loghdr_blkno);

	if (loghdr_meta->ext_used) {
		io_bh->b_data = loghdr_meta->loghdr_ext;
		io_bh->b_size = loghdr_meta->ext_used;
		io_bh->b_offset = sizeof(struct logheader);
		mlfs_write(io_bh);
	}

	bh_release(io_bh);

	//pthread_spin_unlock(&io_bh->b_spinlock);
}

// called at the start of each FS system call.
void start_log_tx(void)
{
	struct logheader_meta *loghdr_meta;

#ifndef CONCURRENT
	pthread_mutex_lock(g_log_mutex_shared);
	g_fs_log->outstanding++;

	mlfs_debug("start log_tx %u\n", g_fs_log->outstanding);
	mlfs_assert(g_fs_log->outstanding == 1);
#endif

	loghdr_meta = get_loghdr_meta();
	memset(loghdr_meta, 0, sizeof(struct logheader_meta));

	if (!loghdr_meta)
		panic("cannot locate logheader_meta\n");

	loghdr_meta->hdr_blkno = 0;
	INIT_LIST_HEAD(&loghdr_meta->link);

	/*
	if (g_fs_log->outstanding == 0 ) {
		mlfs_debug("outstanding %d\n", g_fs_log->outstanding);
		panic("outstanding\n");
	}
	*/
}

void abort_log_tx(void)
{
	struct logheader_meta *loghdr_meta;

	loghdr_meta = get_loghdr_meta();

	if (loghdr_meta->is_hdr_allocated)
		mlfs_free(loghdr_meta->loghdr);

#ifndef CONCURRENT
	pthread_mutex_unlock(g_log_mutex_shared);
	g_fs_log->outstanding--;
#endif

	return;
}

// called at the end of each FS system call.
// commits if this was the last outstanding operation.
void commit_log_tx(void)
{
	int do_commit = 0;

	/*
	if(g_fs_log->outstanding > 0) {
		do_commit = 1;
	} else {
		panic("commit when no outstanding tx\n");
	}
	*/

	do_commit = 1;

	if(do_commit) {
		uint64_t tsc_begin;
		struct logheader_meta *loghdr_meta;

		if (enable_perf_stats)
			tsc_begin = asm_rdtscp();

		commit_log();

		loghdr_meta = get_loghdr_meta();

		if (loghdr_meta->is_hdr_allocated)
			mlfs_free(loghdr_meta->loghdr);
		
#ifndef CONCURRENT
		g_fs_log->outstanding--;
		pthread_mutex_unlock(g_log_mutex_shared);
		mlfs_debug("commit log_tx %u\n", g_fs_log->outstanding);
#endif
		//if (enable_perf_stats) {
			//g_perf_stats.log_commit_tsc += (asm_rdtscp() - tsc_begin);
			//g_perf_stats.log_commit_nr++;
		//}
	} else {
		panic("it has a race condition\n");
	}
}

static int persist_log_inode(struct logheader_meta *loghdr_meta, uint32_t idx)
{
	struct inode *ip;
	addr_t logblk_no;
	uint32_t nr_logblocks = 0;
	struct buffer_head *log_bh;
	struct logheader *loghdr = loghdr_meta->loghdr;
	uint64_t start_tsc;

	logblk_no = loghdr_meta->log_blocks + loghdr_meta->pos;
	loghdr_meta->pos++;

	mlfs_assert(loghdr_meta->pos <= loghdr_meta->nr_log_blocks);

	if (enable_perf_stats)
		start_tsc = asm_rdtscp();

	log_bh = bh_get_sync_IO(g_fs_log->dev, logblk_no, BH_NO_DATA_ALLOC);

	//if (enable_perf_stats) {
		//g_perf_stats.bcache_search_tsc += (asm_rdtscp() - start_tsc);
		//g_perf_stats.bcache_search_nr++;
	//}

	loghdr->blocks[idx] = logblk_no;

	ip = icache_find(g_root_dev, loghdr->inode_no[idx]);
	mlfs_assert(ip);

	log_bh->b_data = (uint8_t *)ip->_dinode;
	log_bh->b_size = sizeof(struct dinode);
	log_bh->b_offset = 0;

	if (ip->flags & I_DELETING) {
		// icache_del(ip);
		// ideleted_add(ip);
		ip->flags &= ~I_VALID;
		bitmap_clear(sb[ip->dev]->s_inode_bitmap, ip->inum, 1);
	}

	nr_logblocks = 1;

	if (!loghdr_meta->secure_log) {
		mlfs_assert(log_bh->b_blocknr < g_fs_log->next_avail);
		mlfs_assert(log_bh->b_dev == g_fs_log->dev);
	}

	mlfs_debug("inum %u offset %lu @ blockno %lx\n",
				loghdr->inode_no[idx], loghdr->data[idx], logblk_no);

	mlfs_write(log_bh);

	bh_release(log_bh);

	//mlfs_assert((log_bh->b_blocknr + nr_logblocks) == g_fs_log->next_avail);

	return 0;
}

static int persist_log_directory_unopt(struct logheader_meta *loghdr_meta, uint32_t idx)
{
	struct inode *dir_ip;
	uint8_t *dirent_array;
	addr_t logblk_no;
	uint32_t nr_logblocks = 0;
	struct buffer_head *log_bh;
	struct logheader *loghdr = loghdr_meta->loghdr;

	logblk_no = log_alloc(1);
	loghdr->blocks[idx] = logblk_no;

	log_bh = bh_get_sync_IO(g_fs_log->dev, logblk_no, BH_NO_DATA_ALLOC);

	dir_ip = icache_find(g_root_dev, loghdr->inode_no[idx]);
	mlfs_assert(dir_ip);

	dirent_array = get_dirent_block(dir_ip, 0);
	mlfs_assert(dirent_array);

	//pthread_spin_lock(&log_bh->b_spinlock);

	log_bh->b_data = dirent_array;
	log_bh->b_size = g_block_size_bytes;

	nr_logblocks = 1;

	if (!loghdr_meta->secure_log) {
		mlfs_assert(log_bh->b_blocknr < g_fs_log->next_avail);
		mlfs_assert(log_bh->b_dev == g_fs_log->dev);
	}
	mlfs_debug("inum %u offset %lu @ blockno %lx\n",
				loghdr->inode_no[idx], loghdr->data[idx], logblk_no);

	// write data to log synchronously
	mlfs_write(log_bh);  

	bh_release(log_bh);

	//pthread_spin_unlock(&log_bh->b_spinlock);

	mlfs_assert((log_bh->b_blocknr + nr_logblocks) == g_fs_log->next_avail);

	return 0;
}

/* This is a critical path for write performance.
 * Stay optimized and need to be careful when modifying it */
static int persist_log_file(struct logheader_meta *loghdr_meta, 
		uint32_t idx, uint8_t n_iovec)
{
	uint32_t k, l, size;
	offset_t key;
	struct fcache_block *fc_block;
	addr_t logblk_no;
	uint32_t nr_logblocks = 0;
	struct buffer_head *log_bh;
	struct logheader *loghdr = loghdr_meta->loghdr;
	uint32_t io_size;
	struct inode *inode;
	lru_key_t lru_entry;
	uint64_t start_tsc, start_tsc_tmp;
	int ret;

	inode = icache_find(g_root_dev, loghdr->inode_no[idx]);

	mlfs_assert(inode);

	size = loghdr_meta->io_vec[n_iovec].size;

	// Handling small write (< 4KB).
	if (size < g_block_size_bytes) {
		// fc_block invalidation and coalescing.
		// 1. find fc_block -> if not exist, allocate fc_block and perform writes
		// -> if exist, fc_block may or may not be valid.
		// 2. if fc_block is valid, then do coalescing.
		// 3. if fc_block is not valid, then skip coalescing and update fc_block.

		uint32_t offset_in_block;

		key = (loghdr->data[idx] >> g_block_size_shift);
		offset_in_block = (loghdr->data[idx] % g_block_size_bytes);

		if (enable_perf_stats)
			start_tsc = asm_rdtscp();

		fc_block = fcache_find(inode, key);

		//if (enable_perf_stats) {
			//g_perf_stats.l0_search_tsc += (asm_rdtscp() - start_tsc);
			//g_perf_stats.l0_search_nr++;
		//}

		logblk_no = loghdr_meta->log_blocks + loghdr_meta->pos;
		loghdr_meta->pos++;

		if (fc_block) {
			ret = check_log_invalidation(fc_block);
			// fc_block is invalid. update it
			if (ret) {
				fc_block->log_version = g_fs_log->avail_version;
				fc_block->log_addr = logblk_no;
			}
			// fc_block is valid
			else {
				if (fc_block->log_addr)  {
					logblk_no = fc_block->log_addr;
					fc_block->log_version = g_fs_log->avail_version;
					mlfs_debug("write is coalesced %lu @ %lu\n", loghdr->data[idx], logblk_no);
				}
			}
		}

		if (!fc_block) {
			mlfs_assert(loghdr_meta->pos <= loghdr_meta->nr_log_blocks);

			fc_block = fcache_alloc_add(inode, key, logblk_no);
			fc_block->log_version = g_fs_log->avail_version;
		} 
		
		if (enable_perf_stats)
			start_tsc = asm_rdtscp();

		log_bh = bh_get_sync_IO(g_fs_log->dev, logblk_no, BH_NO_DATA_ALLOC);

		//if (enable_perf_stats) {
			//g_perf_stats.bcache_search_tsc += (asm_rdtscp() - start_tsc);
			//g_perf_stats.bcache_search_nr++;
		//}

		// the logblk_no could be either a new block or existing one (patching case).
		loghdr->blocks[idx] = logblk_no;

		// case 1. the IO fits into one block.
		if (offset_in_block + size <= g_block_size_bytes)
			io_size = size;
		// case 2. the IO incurs two blocks write (unaligned).
		else 
			panic("do not support this case yet\n");

		log_bh->b_data = loghdr_meta->io_vec[n_iovec].base;
		log_bh->b_size = io_size;
		log_bh->b_offset = offset_in_block;

		mlfs_assert(log_bh->b_dev == g_fs_log->dev);

		mlfs_debug("inum %u offset %lu @ blockno %lx (partial io_size=%u)\n",
				loghdr->inode_no[idx], loghdr->data[idx], logblk_no, io_size);

		mlfs_write(log_bh); 

		bh_release(log_bh);
	} 
	// Handling large (possibly multi-block) write.
	else {
		offset_t cur_offset;

		if (enable_perf_stats)
			start_tsc_tmp = asm_rdtscp();

		cur_offset = loghdr->data[idx];

		/* logheader of multi-block is always 4K aligned.
		 * It is guaranteed by mlfs_file_write() */
		mlfs_assert((loghdr->data[idx] % g_block_size_bytes) == 0);
		mlfs_assert((size % g_block_size_bytes) == 0);

		nr_logblocks = size >> g_block_size_shift; 

		mlfs_assert(nr_logblocks > 0);

		logblk_no = loghdr_meta->log_blocks + loghdr_meta->pos;
		loghdr_meta->pos += nr_logblocks;

		mlfs_assert(loghdr_meta->pos <= loghdr_meta->nr_log_blocks);

		log_bh = bh_get_sync_IO(g_fs_log->dev, logblk_no, BH_NO_DATA_ALLOC);

		log_bh->b_data = loghdr_meta->io_vec[n_iovec].base;
		log_bh->b_size = size;
		log_bh->b_offset = 0;

		loghdr->blocks[idx] = logblk_no;

		// Update log address hash table.
		// This is performance bottleneck of sequential write.
		for (k = 0, l = 0; l < size; l += g_block_size_bytes, k++) {
			key = (cur_offset + l) >> g_block_size_shift;

			mlfs_assert(logblk_no);

			if (enable_perf_stats)
				start_tsc = asm_rdtscp();

			fc_block = fcache_find(inode, key);

			//if (enable_perf_stats) {
				//g_perf_stats.l0_search_tsc += (asm_rdtscp() - start_tsc);
				//g_perf_stats.l0_search_nr++;
			//}

			if (!fc_block) {
				fc_block = fcache_alloc_add(inode, key, logblk_no + k);
				fc_block->log_version = g_fs_log->avail_version;
			} else {
				fc_block->log_version = g_fs_log->avail_version;
				fc_block->log_addr = logblk_no + k;
			}
		}

		mlfs_debug("inum %u offset %lu size %u @ blockno %lx (aligned)\n",
				loghdr->inode_no[idx], cur_offset, size, logblk_no);

		mlfs_write(log_bh);

		bh_release(log_bh);

		//if (enable_perf_stats) {
			//g_perf_stats.tmp_tsc += (asm_rdtscp() - start_tsc_tmp);
		//}
	}

	return 0;
}

static uint32_t compute_log_blocks(struct logheader_meta *loghdr_meta)
{
	struct logheader *loghdr = loghdr_meta->loghdr; 
	uint8_t type, n_iovec; 
	uint32_t nr_log_blocks = 0;
	int i;

	for (i = 0, n_iovec = 0; i < loghdr->n; i++) {
		type = loghdr->type[i];

		switch(type) {
			case L_TYPE_UNLINK:
			case L_TYPE_INODE_CREATE:
			case L_TYPE_INODE_UPDATE: {
				nr_log_blocks++;
				break;
			} 
			case L_TYPE_DIR_ADD: {
				break;
			}
			case L_TYPE_DIR_RENAME: {
				break;
			}
			case L_TYPE_DIR_DEL: {
				break;
			}
			case L_TYPE_FILE: {
				uint32_t size;
				size = loghdr_meta->io_vec[n_iovec].size;

				if (size < g_block_size_bytes)
					nr_log_blocks++;
				else {
                    mlfs_assert(!(size % g_block_size_bytes));
					nr_log_blocks += 
						(size >> g_block_size_shift);
                }
				n_iovec++;
				break;
			}
			default: {
				panic("unsupported log type\n");
				break;
			}
		}
	}

	return nr_log_blocks;
}

// Copy modified blocks from cache to log.
static void persist_log_blocks(struct logheader_meta *loghdr_meta)
{ 
	struct logheader *loghdr = loghdr_meta->loghdr; 
	uint32_t i, nr_logblocks = 0; 
	uint8_t type, n_iovec; 
	addr_t logblk_no; 

	//mlfs_assert(hdr_blkno >= g_fs_log->start);

	for (i = 0, n_iovec = 0; i < loghdr->n; i++) {
		type = loghdr->type[i];

		switch(type) {
			case L_TYPE_UNLINK:
			case L_TYPE_INODE_CREATE:
			case L_TYPE_INODE_UPDATE: {
				persist_log_inode(loghdr_meta, i);
				break;
			} 
				/* Directory information is piggy-backed in 
				 * log header */
			case L_TYPE_DIR_ADD: {
				break;
			}
			case L_TYPE_DIR_RENAME: {
				break;
			}
			case L_TYPE_DIR_DEL: {
				break;
			}
			case L_TYPE_FILE: {
				persist_log_file(loghdr_meta, i, n_iovec);
				n_iovec++;
				break;
			}
			default: {
				panic("unsupported log type\n");
				break;
			}
		}
	}
}

static void commit_log(void)
{
	struct logheader_meta *loghdr_meta;
	struct logheader *loghdr;
	uint64_t tsc_begin, tsc_end;

	// loghdr_meta is stored in TLS.
	loghdr_meta = get_loghdr_meta();
	mlfs_assert(loghdr_meta);

	/* There was no log update during transaction */
	if (!loghdr_meta->is_hdr_allocated)
		return;

	mlfs_assert(loghdr_meta->loghdr);
	loghdr = loghdr_meta->loghdr;

	if (loghdr->n <= 0)
		panic("empty log header\n");

	if (loghdr->n > 0) {
		uint32_t nr_log_blocks;

		// Pre-compute required log blocks for atomic append.
		nr_log_blocks = compute_log_blocks(loghdr_meta);
		nr_log_blocks++; // +1 for a next log header block;

		pthread_mutex_lock(g_fs_log->shared_log_lock);

		// atomic log allocation.
		loghdr_meta->log_blocks = log_alloc(nr_log_blocks);
		loghdr_meta->nr_log_blocks = nr_log_blocks;
		// loghdr_meta->pos = 0 is used for log header block.
		loghdr_meta->pos = 1;

		loghdr_meta->hdr_blkno = loghdr_meta->secure_log ? g_fs_log_secure->next_avail_header : g_fs_log->next_avail_header;
		if (loghdr_meta->secure_log) {
			mlfs_assert(g_fs_log->digesting);
			g_fs_log_secure->next_avail_header = loghdr_meta->log_blocks + loghdr_meta->nr_log_blocks;
		} else {
			g_fs_log->next_avail_header = loghdr_meta->log_blocks + loghdr_meta->nr_log_blocks;
		}

		loghdr->next_loghdr_blkno = loghdr_meta->secure_log ? g_fs_log_secure->next_avail_header : g_fs_log->next_avail_header;
		loghdr->inuse = LH_COMMIT_MAGIC;

		pthread_mutex_unlock(g_fs_log->shared_log_lock);

		mlfs_debug("pid %u [commit] log block %lu nr_log_blocks %u\n",
				getpid(), loghdr_meta->log_blocks, loghdr_meta->nr_log_blocks);
		mlfs_debug("pid %u [commit] current header %lu next header %lu\n", 
				getpid(), loghdr_meta->hdr_blkno, g_fs_log->next_avail_header);

		if (enable_perf_stats)
			tsc_begin = asm_rdtscp();

		/* Crash consistent order: log blocks write
		 * is followed by log header write */
		persist_log_blocks(loghdr_meta);

		//if (enable_perf_stats) {
			//tsc_end = asm_rdtscp();
			//g_perf_stats.log_write_tsc += (tsc_end - tsc_begin);
		//}

#if 0
		if(loghdr->next_loghdr_blkno != g_fs_log->next_avail_header) {
			printf("loghdr_blkno %lu, next_avail %lu\n",
					loghdr->next_loghdr_blkno, g_fs_log->next_avail_header);
			panic("loghdr->next_loghdr_blkno is tainted\n");
		}
#endif

		if (enable_perf_stats)
			tsc_begin = asm_rdtscp();

		// Write log header to log area (real commit)
		persist_log_header(loghdr_meta, loghdr_meta->hdr_blkno);

		//if (enable_perf_stats) {
			//tsc_end = asm_rdtscp();
			//g_perf_stats.loghdr_write_tsc += (tsc_end - tsc_begin);
		//}

		if (!loghdr_meta->secure_log) {
			atomic_fetch_add(&g_log_sb->n_digest, 1);
		} else {
			atomic_fetch_add(&g_fs_log_secure->n_digest, 1);
		}

		mlfs_assert(loghdr_meta->loghdr->next_loghdr_blkno
				>= g_fs_log->log_sb_blk);
	}
}

void add_to_loghdr(uint8_t type, struct inode *inode, offset_t data, 
		uint32_t length, void *extra, uint16_t extra_len)
{
	uint32_t i;
	struct logheader *loghdr;
	struct logheader_meta *loghdr_meta;

	loghdr_meta = get_loghdr_meta();

	mlfs_assert(loghdr_meta);

	if (!loghdr_meta->is_hdr_allocated) {
		loghdr = (struct logheader *)mlfs_zalloc(sizeof(*loghdr));

		loghdr_meta->loghdr = loghdr;
		loghdr_meta->is_hdr_allocated = 1;
	}

	loghdr = loghdr_meta->loghdr;

	if (loghdr->n >= g_fs_log->size && !loghdr_meta->secure_log)
		panic("too big a transaction for log");
	if (loghdr_meta->secure_log)
		mlfs_assert(loghdr->n < g_fs_log_secure->size);

	/*
		 if (g_fs_log->outstanding < 1)
		 panic("add_to_loghdr: outside of trans");
		 */

	i = loghdr->n;

	if (i >= g_max_blocks_per_operation)
		panic("log header is too small\n");

	loghdr->type[i] = type;
	loghdr->inode_no[i] = inode->inum;

	if (type == L_TYPE_FILE) 
		// offset in file.
		loghdr->data[i] = (offset_t)data;
	else if (type == L_TYPE_DIR_ADD ||
			type == L_TYPE_DIR_RENAME ||
			type == L_TYPE_DIR_DEL) {
		// dirent inode number.
		loghdr->data[i] = (uint32_t)data;
	} else
		loghdr->data[i] = 0;

	loghdr->length[i] = length;
	loghdr->n++;

	if (extra_len) {
		uint16_t ext_used = loghdr_meta->ext_used;

		loghdr_meta->loghdr_ext[ext_used] =  '0' + i;
		ext_used++;
		memmove(&loghdr_meta->loghdr_ext[ext_used], extra, extra_len);
		ext_used += extra_len;
		strncat((char *)&loghdr_meta->loghdr_ext[ext_used], "|", 1);
		ext_used++;
		loghdr_meta->loghdr_ext[ext_used] = '\0';
		loghdr_meta->ext_used = ext_used;

		mlfs_assert(ext_used <= 2048);
	}

	/*
		 if (type != L_TYPE_FILE)
		 mlfs_debug("[loghdr-add] dev %u, type %u inum %u data %lu\n",
		 inode->dev, type, inode->inum, data);
		 */
}

////////////////////////////////////////////////////////////////////
// Libmlfs digest thread.

void wait_on_digesting()
{
	uint64_t tsc_begin, tsc_end;
	if (enable_perf_stats) 
		tsc_begin = asm_rdtsc();

	while(g_fs_log->digesting)
		cpu_relax();

	//if (enable_perf_stats) {
		//tsc_end = asm_rdtsc();
		//g_perf_stats.digest_wait_tsc += (tsc_end - tsc_begin);
		//g_perf_stats.digest_wait_nr++;
	//}
}

int make_digest_request_async(int percent)
{
	char cmd_buf[MAX_CMD_BUF] = {0};
	int ret = 0;

	sprintf(cmd_buf, "|digest |%d|", percent);

	if (!g_fs_log->digesting && atomic_load(&g_log_sb->n_digest) > 0) {
		set_digesting();
		mlfs_debug("Send digest command: %s\n", cmd_buf);
		ret = write(g_fs_log->digest_fd[1], cmd_buf, MAX_CMD_BUF);
		return 0;
	} else
		return -EBUSY;
}

void coalesce_replay_and_optimize(uint8_t from_dev, 
		loghdr_meta_t *loghdr_meta, struct replay_list *replay_list)
{	
	int i, ret;
	loghdr_t *loghdr;
	uint16_t nr_entries;

	nr_entries = loghdr_meta->loghdr->n;
	loghdr = loghdr_meta->loghdr;

	for (i = 0; i < nr_entries; ++i) {
		switch(loghdr->type[i]) {
			case L_TYPE_INODE_CREATE:
			case L_TYPE_INODE_UPDATE: {
				i_replay_t search, *item;
				memset(&search, 0, sizeof(i_replay_t));

				search.key.inum = loghdr->inode_no[i];

				if (loghdr->type[i] == L_TYPE_INODE_CREATE) 
						inode_version_table[search.key.inum]++;
				search.key.ver = inode_version_table[search.key.inum];

				HASH_FIND(hh, replay_list->i_digest_hash, &search.key,
						sizeof(replay_key_t), item);
				if (!item) {
					item = (i_replay_t *)mlfs_zalloc(sizeof(i_replay_t));
					item->key = search.key;
					item->node_type = NTYPE_I;
					list_add_tail(&item->list, &replay_list->head);

					// tag the inode coalecing starts from inode creation.
					// This is crucial information to decide whether 
					// unlink can skip or not.
					if (loghdr->type[i] == L_TYPE_INODE_CREATE) 
						item->create = 1;
					else
						item->create = 0;

					HASH_ADD(hh, replay_list->i_digest_hash, key,
							sizeof(replay_key_t), item);
					mlfs_debug("[INODE] inum %u (ver %u) - create %d\n",
							item->key.inum, item->key.ver, item->create);
				}
				// move blknr to point the up-to-date inode snapshot in the log.
				item->blknr = loghdr->blocks[i];
				/*if (enable_perf_stats)*/
					/*g_perf_stats.n_digest++;*/
				break;
			}
			case L_TYPE_DIR_DEL: 
			case L_TYPE_DIR_RENAME: 
			case L_TYPE_DIR_ADD: {
				d_replay_t search, *item;
				// search must be zeroed at the beginning.
				memset(&search, 0, sizeof(d_replay_t));
				search.key.inum = loghdr->data[i];
				search.key.type = loghdr->type[i];
				search.key.ver = inode_version_table[loghdr->data[i]];

				HASH_FIND(hh, replay_list->d_digest_hash, &search.key,
						sizeof(d_replay_key_t), item);
				if (!item) {
					item = (d_replay_t *)mlfs_zalloc(sizeof(d_replay_t));
					item->key = search.key;
					HASH_ADD(hh, replay_list->d_digest_hash, key, 
							sizeof(d_replay_key_t), item);
					item->node_type = NTYPE_D;
					list_add_tail(&item->list, &replay_list->head);
					mlfs_debug("[ DIR ] inum %u (ver %u) - %s\n",
							item->key.inum, 
							item->key.ver,
							loghdr->type[i] == L_TYPE_DIR_ADD ? "ADD" : 
							loghdr->type[i] == L_TYPE_DIR_DEL ? "DEL" :
							"RENAME");
				}
				item->n = i;
				item->dir_inum = loghdr->inode_no[i];
				item->dir_size = loghdr->length[i];
				item->blknr = loghdr_meta->hdr_blkno;
				//if (enable_perf_stats)
					//g_perf_stats.n_digest++;
				break;
			}
			case L_TYPE_FILE: {
				f_replay_t search, *item;
				f_iovec_t *f_iovec;
				f_blklist_t *_blk_list;
				lru_key_t k;
				offset_t iovec_key;
				int found = 0;

				memset(&search, 0, sizeof(f_replay_t));
				search.key.inum = loghdr->inode_no[i];
				search.key.ver = inode_version_table[loghdr->inode_no[i]];

				HASH_FIND(hh, replay_list->f_digest_hash, &search.key,
						sizeof(replay_key_t), item);
				if (!item) {
					item = (f_replay_t *)mlfs_zalloc(sizeof(f_replay_t));
					item->key = search.key;

					HASH_ADD(hh, replay_list->f_digest_hash, key,
							sizeof(replay_key_t), item);

					INIT_LIST_HEAD(&item->iovec_list);
					item->node_type = NTYPE_F;
					item->iovec_hash = NULL;
					list_add_tail(&item->list, &replay_list->head);
				}

#ifndef EXPERIMENTAL
#ifdef IOMERGE
				// IO data is merged if the same offset found.
				// Reduce amount IO when IO data has locality such as Zipf dist.
				// FIXME: currently iomerge works correctly when IO size is 
				// 4 KB and aligned.
				iovec_key = ALIGN_FLOOR(loghdr->data[i], g_block_size_bytes);

				if (loghdr->data[i] % g_block_size_bytes !=0 ||
						loghdr->length[i] != g_block_size_bytes) 
					panic("IO merge is not support current IO pattern\n");

				HASH_FIND(hh, item->iovec_hash, 
						&iovec_key, sizeof(offset_t), f_iovec);

				if (f_iovec && 
						(f_iovec->length == loghdr->length[i])) {
					f_iovec->offset = iovec_key;
					f_iovec->blknr = loghdr->blocks[i];
					// TODO: merge data from loghdr->blocks to f_iovec buffer.
					found = 1;
				}

				if (!found) {
					f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
					f_iovec->length = loghdr->length[i];
					f_iovec->offset = loghdr->data[i];
					f_iovec->blknr = loghdr->blocks[i];
					INIT_LIST_HEAD(&f_iovec->list);
					list_add_tail(&f_iovec->list, &item->iovec_list);

					f_iovec->hash_key = iovec_key;
					HASH_ADD(hh, item->iovec_hash, hash_key,
							sizeof(offset_t), f_iovec);
				}
#else
				f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
				f_iovec->length = loghdr->length[i];
				f_iovec->offset = loghdr->data[i];
				f_iovec->blknr = loghdr->blocks[i];
				INIT_LIST_HEAD(&f_iovec->list);
				list_add_tail(&f_iovec->list, &item->iovec_list);
#endif	//IOMERGE

#else //EXPERIMENTAL
				// Experimental feature: merge contiguous small writes to
				// a single write one.
				mlfs_debug("new log block %lu\n", loghdr->blocks[i]);
				_blk_list = (f_blklist_t *)mlfs_zalloc(sizeof(f_blklist_t));
				INIT_LIST_HEAD(&_blk_list->list);

				// FIXME: Now only support 4K aligned write.
				_blk_list->n = (loghdr->length[i] >> g_block_size_shift);
				_blk_list->blknr = loghdr->blocks[i];

				if (!list_empty(&item->iovec_list)) {
					f_iovec = list_last_entry(&item->iovec_list, f_iovec_t, list);

					// Find the case where io_vector can be coalesced.
					if (f_iovec->offset + f_iovec->length == loghdr->data[i]) {
						f_iovec->length += loghdr->length[i];
						f_iovec->n_list++;

						mlfs_debug("block is merged %u\n", _blk_list->blknr);
						list_add_tail(&_blk_list->list, &f_iovec->iov_blk_list);
					} else {
						mlfs_debug("new f_iovec %lu\n",  loghdr->data[i]); 
						// cannot coalesce io_vector. allocate new one.
						f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
						f_iovec->length = loghdr->length[i];
						f_iovec->offset = loghdr->data[i];
						f_iovec->blknr = loghdr->blocks[i];
						INIT_LIST_HEAD(&f_iovec->list);
						INIT_LIST_HEAD(&f_iovec->iov_blk_list);

						list_add_tail(&_blk_list->list, &f_iovec->iov_blk_list);
						list_add_tail(&f_iovec->list, &item->iovec_list);
					}
				} else {
					f_iovec = (f_iovec_t *)mlfs_zalloc(sizeof(f_iovec_t));
					f_iovec->length = loghdr->length[i];
					f_iovec->offset = loghdr->data[i];
					f_iovec->blknr = loghdr->blocks[i];
					INIT_LIST_HEAD(&f_iovec->list);
					INIT_LIST_HEAD(&f_iovec->iov_blk_list);

					list_add_tail(&_blk_list->list, &f_iovec->iov_blk_list);
					list_add_tail(&f_iovec->list, &item->iovec_list);
				}
#endif

				//if (enable_perf_stats)
					//g_perf_stats.n_digest++;
				break;
			}
			case L_TYPE_UNLINK: {
				// Got it! Kernfs can skip digest of related items.
				// clean-up inode, directory, file digest operations for the inode.
				uint32_t inum = loghdr->inode_no[i];
				i_replay_t i_search, *i_item;
				d_replay_t d_search, *d_item;
				f_replay_t f_search, *f_item;
				u_replay_t u_search, *u_item;
				//d_replay_key_t d_key;
				f_iovec_t *f_iovec, *tmp;

				replay_key_t key = {
					.inum = loghdr->inode_no[i],
					.ver = inode_version_table[loghdr->inode_no[i]],
				};

				// This is required for structure key in UThash.
				memset(&i_search, 0, sizeof(i_replay_t));
				memset(&d_search, 0, sizeof(d_replay_t));
				memset(&f_search, 0, sizeof(f_replay_t));
				memset(&u_search, 0, sizeof(u_replay_t));

				mlfs_debug("%s\n", "-------------------------------");

				// check inode digest info can skip.
				i_search.key.inum = key.inum;
				i_search.key.ver = key.ver;
				HASH_FIND(hh, replay_list->i_digest_hash, &i_search.key,
						sizeof(replay_key_t), i_item);

				if (i_item && i_item->create) {
					mlfs_debug("[INODE] inum %u (ver %u) --> SKIP\n", 
							i_item->key.inum, i_item->key.ver);
					// the unlink can skip and erase related i_items
					HASH_DEL(replay_list->i_digest_hash, i_item);
					list_del(&i_item->list);
					mlfs_free(i_item);
					//if (enable_perf_stats)
						//g_perf_stats.n_digest_skipped++;
				} else {
					// the unlink must be applied. create a new unlink item.
					u_item = (u_replay_t *)mlfs_zalloc(sizeof(u_replay_t));
					u_item->key = key;
					u_item->node_type = NTYPE_U;
					HASH_ADD(hh, replay_list->u_digest_hash, key,
							sizeof(replay_key_t), u_item);
					list_add_tail(&u_item->list, &replay_list->head);
					mlfs_debug("[ULINK] inum %u (ver %u)\n", 
							u_item->key.inum, u_item->key.ver);
					//if (enable_perf_stats)
						//g_perf_stats.n_digest++;
				}

#if 0
				HASH_FIND(hh, replay_list->u_digest_hash, &key,
						sizeof(replay_key_t), u_item);
				if (u_item) {
					// previous unlink can skip. 
					mlfs_debug("[ULINK] inum %u (ver %u) --> SKIP\n", 
							u_item->key.inum, u_item->key.ver);
					HASH_DEL(replay_list->u_digest_hash, u_item);
					list_del(&u_item->list);
					mlfs_free(u_item);
				} 
#endif

				// check directory digest info to skip.
				d_search.key.inum = inum;
				d_search.key.ver = key.ver;
				d_search.key.type = L_TYPE_DIR_ADD;

				HASH_FIND(hh, replay_list->d_digest_hash, &d_search.key,
						sizeof(d_replay_key_t), d_item);
				
				if (d_item) {
					mlfs_debug("[ DIR ] inum %u (ver %u) - ADD --> SKIP\n", 
							d_item->key.inum, d_item->key.ver);
					HASH_DEL(replay_list->d_digest_hash, d_item);
					list_del(&d_item->list);
					mlfs_free(d_item);
					//if (enable_perf_stats)
						//g_perf_stats.n_digest_skipped++;
				}

				d_search.key.inum = inum;
				d_search.key.ver = key.ver;
				d_search.key.type = L_TYPE_DIR_RENAME;

				HASH_FIND(hh, replay_list->d_digest_hash, &d_search.key,
						sizeof(d_replay_key_t), d_item);
				
				if (d_item) {
					mlfs_debug("[ DIR ] inum %u (ver %u) - RENAME --> SKIP\n", 
							d_item->key.inum, d_item->key.ver);
					HASH_DEL(replay_list->d_digest_hash, d_item);
					list_del(&d_item->list);
					mlfs_free(d_item);
					//if (enable_perf_stats)
						//g_perf_stats.n_digest_skipped++;
				}

				// unlink digest must happens before directory delete digest.
				memset(&d_search, 0, sizeof(d_replay_t));
				d_search.key.inum = inum;
				d_search.key.ver = key.ver;
				d_search.key.type = L_TYPE_DIR_DEL;

				HASH_FIND(hh, replay_list->d_digest_hash, &d_search.key,
						sizeof(d_replay_key_t), d_item);
				
				if (d_item && d_item->key.ver != 0 ) {
					mlfs_debug("[ DIR ] inum %u (ver %u) - DEL --> SKIP\n", 
							d_item->key.inum, d_item->key.ver);
					HASH_DEL(replay_list->d_digest_hash, d_item);
					list_del(&d_item->list);
					mlfs_free(d_item);
					
					//if (enable_perf_stats)
						//g_perf_stats.n_digest_skipped++;
				}

				// delete file digest info.
				f_search.key.inum = key.inum;
				f_search.key.ver = key.ver;

				HASH_FIND(hh, replay_list->f_digest_hash, &f_search.key,
						sizeof(replay_key_t), f_item);

				if (f_item) {
					list_for_each_entry_safe(f_iovec, tmp, 
							&f_item->iovec_list, list) {
						list_del(&f_iovec->list);
						mlfs_free(f_iovec);
						
						//if (enable_perf_stats)
							//g_perf_stats.n_digest_skipped++;
					}

					HASH_DEL(replay_list->f_digest_hash, f_item);
					list_del(&f_item->list);
					mlfs_free(f_item);
				}

				mlfs_debug("%s\n", "-------------------------------");
				break;
			}
			default: {
				printf("%s: digest type %d\n", __func__, loghdr->type[i]);
				panic("unsupported type of operation\n");
				break;
			}
		}
	}
}

void print_replay_list(struct replay_list *replay_list)
{
	struct list_head *l, *tmp;
	uint8_t *node_type;
	f_iovec_t *f_iovec, *iovec_tmp;
	uint64_t tsc_begin;

	list_for_each_safe(l, tmp, &replay_list->head) {
		node_type = (uint8_t *)l + sizeof(struct list_head);
		mlfs_assert(*node_type < 5);

		switch (*node_type) {
			case NTYPE_I: {
				i_replay_t *i_item;
				i_item = (i_replay_t *)container_of(l, i_replay_t, list);

				// if (enable_perf_stats) 
				// 	tsc_begin = asm_rdtscp();

				// digest_inode(from_dev, g_root_dev, i_item->key.inum, i_item->blknr);

				printf("\tstruct inode_replay\n");
				printf("\t\taddr_t blknr: %x\n", i_item->blknr);
				printf("\t\tuint8_t node_type: %d\n", i_item->node_type);
				printf("\t\tuint8_t create: %d\n", i_item->create);
				printf("\t\tuint32_t inum: %d\n", i_item->key.inum);
				printf("\t\tuint16_t ver: %d", i_item->key.ver);
				HASH_DEL(replay_list->i_digest_hash, i_item);
				list_del(l);
				mlfs_free(i_item);

				// if (enable_perf_stats)
				// 	g_perf_stats.digest_inode_tsc += asm_rdtscp() - tsc_begin;
				
				break;
			}
			case NTYPE_D: {
				d_replay_t *d_item;
				d_item = (d_replay_t *)container_of(l, d_replay_t, list);

				// if (enable_perf_stats) 
				// 	tsc_begin = asm_rdtscp();

				// digest_directory(from_dev, g_root_dev, d_item->n, 
				// 		d_item->key.type, d_item->dir_inum, d_item->dir_size, 
				// 		d_item->key.inum, d_item->blknr);

				printf("\tstruct directory_replay\n");
				printf("\t\tint n: %d\n", d_item->n);
				printf("\t\tuint32_t dir_inum: %d\n", d_item->dir_inum);
				printf("\t\tuint32_t dir_size: %d\n" ,d_item->dir_size);
				printf("\t\taddr_t blknr: %d\n", d_item->blknr);
				printf("\t\tuint8_t node_type: %d\n", d_item->node_type);
				printf("\t\tuint32_t inum: %d\n", d_item->key.inum);
				printf("\t\tuint16_t ver: %d", d_item->key.ver);
				HASH_DEL(replay_list->d_digest_hash, d_item);
				list_del(l);
				mlfs_free(d_item);

				// if (enable_perf_stats)
				// 	g_perf_stats.digest_dir_tsc += asm_rdtscp() - tsc_begin;

				break;
			}
			case NTYPE_F: {
				uint8_t dest_dev = g_root_dev;
				f_replay_t *f_item, *t;
				f_item = (f_replay_t *)container_of(l, f_replay_t, list);
				lru_key_t k;

				// if (enable_perf_stats) 
				// 	tsc_begin = asm_rdtscp();

				printf("\tstruct file_replay\n");
				printf("\t\tuint8_t node_type: %d\n", f_item->node_type);
				printf("\t\tuint32_t inum: %d\n", f_item->key.inum);
				printf("\t\tuint16_t ver: %d", f_item->key.ver);
				printf("\t\tstruct list_head iovec_list:\n");


#ifdef FCONCURRENT
				// HASH_ITER(hh, replay_list->f_digest_hash, f_item, t) {
				// 	struct f_digest_worker_arg *arg;

				// 	// Digest worker thread will free the arg.
				// 	arg = (struct f_digest_worker_arg *)mlfs_alloc(
				// 			sizeof(struct f_digest_worker_arg));

				// 	arg->from_dev = from_dev;
				// 	arg->to_dev = g_root_dev;
				// 	arg->f_item = f_item;

				// 	thpool_add_work(file_digest_thread_pool,
				// 			file_digest_worker, (void *)arg);
				// }

				// //if (thpool_num_threads_working(file_digest_thread_pool))
				// thpool_wait(file_digest_thread_pool);

				// HASH_ITER(hh, replay_list->f_digest_hash, f_item, t) {
				// 	HASH_DEL(replay_list->f_digest_hash, f_item);
				// 	mlfs_free(f_item);
				// }
#else
				list_for_each_entry_safe(f_iovec, iovec_tmp, 
						&f_item->iovec_list, list) {

#ifndef EXPERIMENTAL
					// digest_file(from_dev, dest_dev, 
					// 		f_item->key.inum, f_iovec->offset, 
					// 		f_iovec->length, f_iovec->blknr);
					// mlfs_free(f_iovec);
#else
					// digest_file_iovec(from_dev, dest_dev, 
					// 		f_item->key.inum, f_iovec);
#endif //EXPERIMENTAL
					// if (dest_dev == g_ssd_dev)
					// 	mlfs_io_wait(g_ssd_dev, 0);
					printf("\t\t\tstruct file_io_vector\n");
					printf("\t\t\t\toffset_t offset: %d\n", f_iovec->offset);
					printf("\t\t\t\tuint32_t length: %d\n", f_iovec->length);
					printf("\t\t\t\taddr_t blknr: %d\n", f_iovec->blknr);
					printf("\t\t\t\tuint32_t n_list: %d\n", f_iovec->n_list);
					printf("\t\t\t\tstruct list_head iov_blk_list ???\n");
				}

				HASH_DEL(replay_list->f_digest_hash, f_item);
				mlfs_free(f_item);
#endif //FCONCURRENT

				// list_del(l);

				// if (enable_perf_stats)
				// 	g_perf_stats.digest_file_tsc += asm_rdtscp() - tsc_begin;
				break;
			}
			case NTYPE_U: {
				u_replay_t *u_item;
				u_item = (u_replay_t *)container_of(l, u_replay_t, list);

				// if (enable_perf_stats) 
				// 	tsc_begin = asm_rdtscp();

				// digest_unlink(from_dev, g_root_dev, u_item->key.inum);

				printf("\tstruct unlink_replay\n");
				printf("\t\tuint8_t node_type: %d\n", u_item->node_type);
				printf("\t\tuint32_t inum: %d\n", u_item->key.inum);
				printf("\t\tuint16_t ver: %d", u_item->key.ver);
				HASH_DEL(replay_list->u_digest_hash, u_item);
				list_del(l);
				mlfs_free(u_item);

				// if (enable_perf_stats)
				// 	g_perf_stats.digest_inode_tsc += asm_rdtscp() - tsc_begin;
				break;
			}
			default:
				panic("unsupported node type!\n");
		}
	}
}

void copy_log_from_replay_list(uint8_t from_dev, struct replay_list *replay_list)
{
	struct list_head *l, *tmp;
	struct logheader_meta *loghdr_meta;
	struct inode *ip;
	uint8_t *node_type, *data;
	f_iovec_t *f_iovec, *iovec_tmp;
	uint64_t tsc_begin;

	list_for_each_safe(l, tmp, &replay_list->head) {
				node_type = (uint8_t *)l + sizeof(struct list_head);
		mlfs_assert(*node_type < 5);

		switch(*node_type) {
			case NTYPE_I: {
				i_replay_t *i_item;
				i_item = (i_replay_t *)container_of(l, i_replay_t, list);

				start_log_tx();

				commit_log_tx();


				HASH_DEL(replay_list->i_digest_hash, i_item);
				list_del(l);
				mlfs_free(i_item);
				break;
			}
			case NTYPE_D: {
				d_replay_t *d_item;
				d_item = (d_replay_t *)container_of(l, d_replay_t, list);


				start_log_tx();
				commit_log_tx();

				HASH_DEL(replay_list->d_digest_hash, d_item);
				list_del(l);
				mlfs_free(d_item);
				break;
			}
			case NTYPE_F: {
				uint8_t dest_dev = g_root_dev;
				f_replay_t *f_item, *t;
				f_item = (f_replay_t *)container_of(l, f_replay_t, list);
				lru_key_t k;


// #ifdef FCONCURRENT
// 				HASH_ITER(hh, replay_list->f_digest_hash, f_item, t) {
// 					struct f_digest_worker_arg *arg;

// 					// Digest worker thread will free the arg.
// 					arg = (struct f_digest_worker_arg *)mlfs_alloc(
// 							sizeof(struct f_digest_worker_arg));

// 					arg->from_dev = from_dev;
// 					arg->to_dev = g_root_dev;
// 					arg->f_item = f_item;

// 					thpool_add_work(file_digest_thread_pool,
// 							file_digest_worker, (void *)arg);
// 				}

// 				//if (thpool_num_threads_working(file_digest_thread_pool))
// 				thpool_wait(file_digest_thread_pool);

// 				HASH_ITER(hh, replay_list->f_digest_hash, f_item, t) {
// 					HASH_DEL(replay_list->f_digest_hash, f_item);
// 					mlfs_free(f_item);
// 				}
// #else
				list_for_each_entry_safe(f_iovec, iovec_tmp,
						&f_item->iovec_list, list) {

					start_log_tx();
					loghdr_meta = get_loghdr_meta();
					mlfs_assert(loghdr_meta);
					ip = icache_find(g_root_dev, f_item->key.inum);
					data = g_bdev[from_dev]->map_base_addr + (f_iovec->blknr << g_block_size_shift);
					loghdr_meta->secure_log = 1;
					add_to_log(ip, data, f_iovec->offset, f_iovec->length);
					commit_log_tx();
					mlfs_free(f_iovec);
				}

				HASH_DEL(replay_list->f_digest_hash, f_item);
				mlfs_free(f_item);
				list_del(l);
				break;
			}
			case NTYPE_U: {
				u_replay_t *u_item;
				u_item = (u_replay_t *)container_of(l, u_replay_t, list);


				HASH_DEL(replay_list->u_digest_hash, u_item);
				list_del(l);
				mlfs_free(u_item);
				break;
			}
			default:
				panic("unsupported node type!\n");
		}
	}
}

int coalesce_logs(uint8_t from_dev, int n_hdrs, addr_t *loghdr_to_digest, int *rotated)
{
	loghdr_meta_t *loghdr_meta;
	int i, n_coalesce;
	uint64_t tsc_begin;
	static addr_t previous_loghdr_blk;
	struct replay_list replay_list = {
		.i_digest_hash = NULL,
		.d_digest_hash = NULL,
		.f_digest_hash = NULL,
		.u_digest_hash = NULL,
	};

	INIT_LIST_HEAD(&replay_list.head);
	
	memset(inode_version_table, 0, sizeof(uint16_t) * NINODES);

	// coalesce log entries
	for (i = 0; i < n_hdrs; ++i) {
		loghdr_meta = read_log_header_meta(from_dev, *loghdr_to_digest);

		// if this log header was not committed, then skip over it
		if (loghdr_meta->loghdr->inuse != LH_COMMIT_MAGIC) {
			mlfs_assert(loghdr_meta->loghdr->inuse == 0);
			mlfs_free(loghdr_meta);
			break;
		}

		coalesce_replay_and_optimize(from_dev, loghdr_meta, &replay_list);

		// rotated when next_loghdr_blkno jumps to beginning of the log.
		// FIXME: instead of this condition, it would be better if 
		// *loghdr_to_digest > the lost block of application log.
		if (*loghdr_to_digest > loghdr_meta->loghdr->next_loghdr_blkno) {
			mlfs_debug("loghdr_to_digest %lu, next header %lu\n",
					*loghdr_to_digest, loghdr_meta->loghdr->next_loghdr_blkno);
			*rotated = 1;
		}

		*loghdr_to_digest = loghdr_meta->loghdr->next_loghdr_blkno;

		previous_loghdr_blk = loghdr_meta->hdr_blkno;

		mlfs_free(loghdr_meta);
	}
    //print_replay_list(&replay_list);

	copy_log_from_replay_list(from_dev, &replay_list);

	n_coalesce = i;
	return n_coalesce;
}

uint32_t make_digest_request_sync(int percent)
{
	int ret, i;
	char cmd[MAX_SOCK_BUF];
	uint32_t digest_count = 0, n_digest;
	loghdr_t *loghdr;
	addr_t loghdr_blkno = g_fs_log->start_blk;
	struct inode *ip;

	g_log_sb->start_digest = g_fs_log->start_blk;
	write_log_superblock(g_log_sb);

	n_digest = atomic_load(&g_log_sb->n_digest);

	g_fs_log->n_digest_req = (percent * n_digest) / 100;
	
#ifdef COALESCE
	log_rotated_during_coalescing = 0;
	coalesce_count = 0;
	digest_blkno = g_log_sb->start_digest;
    coalesce_count = coalesce_logs(g_fs_log->dev, g_fs_log->n_digest_req, &digest_blkno, &log_rotated_during_coalescing);
#endif

	socklen_t len = sizeof(struct sockaddr_un);
	sprintf(cmd, "|digest |%d|%u|%lu|%lu|",
			g_fs_log->dev, g_fs_log->n_digest_req, g_log_sb->start_digest, 0UL);
	mlfs_info("%s\n", cmd);

	// send digest command
	ret = sendto(g_sock_fd, cmd, MAX_SOCK_BUF, 0, 
			(struct sockaddr *)&g_srv_addr, len);

	return n_digest;
}

static void cleanup_lru_list(int lru_updated)
{
	lru_node_t *node, *tmp;
	int i = 0;

	pthread_rwlock_wrlock(shm_lru_rwlock);

	list_for_each_entry_safe_reverse(node, tmp, &lru_heads[g_log_dev], list) {
		HASH_DEL(lru_hash, node);
		list_del(&node->list);
		mlfs_free_shared(node);
	}

	pthread_rwlock_unlock(shm_lru_rwlock);
}

void handle_digest_response(char *ack_cmd)
{
	char ack[10] = {0};
	addr_t next_hdr_of_digested_hdr;
	int n_digested, rotated, lru_updated;
	struct inode *inode, *tmp;

	sscanf(ack_cmd, "|%s |%d|%lu|%d|%d|", ack, &n_digested, 
			&next_hdr_of_digested_hdr, &rotated, &lru_updated);

	if (g_fs_log->n_digest_req == n_digested)  {
		mlfs_info("%s", "digest is done correctly\n");
		mlfs_info("%s", "-----------------------------------\n");
	} else {
		mlfs_printf("[D] digest is done insufficiently: req %u | done %u\n",
				g_fs_log->n_digest_req, n_digested);
		panic("Digest was incorrect!\n");
	}

	mlfs_debug("g_fs_log->start_blk %lx, next_hdr_of_digested_hdr %lx\n",
			g_fs_log->start_blk, next_hdr_of_digested_hdr);

	if (rotated || log_rotated_during_coalescing) {
		g_fs_log->start_version++;
		mlfs_debug("g_fs_log start_version = %d\n", g_fs_log->start_version);
	}

	// change start_blk
	g_fs_log->start_blk = next_hdr_of_digested_hdr;
	g_log_sb->start_digest = next_hdr_of_digested_hdr;

	// adjust g_log_sb->n_digest properly
	atomic_fetch_sub(&g_log_sb->n_digest, n_digested);

	// reset the secure log
	g_fs_log_secure->size = disk_sb[dev].nlog;
	g_fs_log_secure->next_avail_header = g_log_sb->secure_start_digest;
	g_fs_log_secure->next_avail = g_log_sb->secure_start_digest + 1;
	g_fs_log_secure->start_blk = g_log_sb->secure_start_digest;
	g_fs_log_secure->start_digest =  g_fs_log_secure->next_avail_header;

	atomic_init(&g_log_sb->n_secure_digest, 0);

	//Start cleanup process after digest is done.

	//cleanup_lru_list(lru_updated);

	// TODO: optimize this. Now sync all inodes in the inode_hash.
	// As the optimization, Kernfs sends inodes lists (via shared memory),
	// and Libfs syncs inodes based on the list.
	HASH_ITER(hash_handle, inode_hash[g_root_dev], inode, tmp) {
		if (!(inode->flags & I_DELETING)) {
			if (inode->itype == T_FILE) 
				sync_inode_ext_tree(g_root_dev, inode);
			else if(inode->itype == T_DIR)
				;
			else
				panic("unsupported inode type\n");
		}
	}

	// persist log superblock.
	write_log_superblock(g_log_sb);

	//xchg_8(&g_fs_log->digesting, 0);
	clear_digesting();

	if (enable_perf_stats) 
		show_libfs_stats();
}

#define EVENT_COUNT 2
void *digest_thread(void *arg)
{
	int epfd, kernfs_epfd, ret, n, flags;
	char buf[MAX_SOCK_BUF] = {0}, cmd_buf[MAX_CMD_BUF] = {0};
	struct epoll_event kernfs_epev = {0}, epev[EVENT_COUNT] = {0};
	struct sockaddr_un srv_addr;

	// setup server address
	memset(&g_srv_addr, 0, sizeof(g_addr));
	g_srv_addr.sun_family = AF_UNIX;
	strncpy(g_srv_addr.sun_path, SRV_SOCK_PATH, sizeof(g_srv_addr.sun_path));

	// Create domain socket for sending digest command to kernfs
	if ((g_sock_fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0)
		panic("fail to create socket\n");

	memset(&g_addr, 0, sizeof(g_addr));
	g_addr.sun_family = AF_UNIX;
	snprintf(g_addr.sun_path, sizeof(g_addr.sun_path), 
			"/tmp/mlfs_cli.%ld", (long) getpid());

	unlink(g_addr.sun_path);

	if (bind(g_sock_fd, (struct sockaddr *)&g_addr, 
				sizeof(struct sockaddr_un)) == -1)
		panic("bind error\n");

	flags = fcntl(g_sock_fd, F_GETFL, 0);
	flags |= O_NONBLOCK;
	ret = fcntl(g_sock_fd, F_SETFL, flags);
	if (ret < 0)
		panic("fail to set non-blocking mode\n");

	// epoll for socket: communication with kernfs
	kernfs_epfd = epoll_create(1);
	if (kernfs_epfd < 0)
		panic("cannot create epoll fd\n");

	kernfs_epev.data.fd = g_sock_fd;
	kernfs_epev.events = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
	ret = epoll_ctl(kernfs_epfd, EPOLL_CTL_ADD, g_sock_fd, 
			&kernfs_epev);
	if (ret < 0)
		panic("fail to connect epoll fd\n");

	// epoll for pipe and kernfs
	epfd = epoll_create(1);
	if (epfd < 0)
		panic("cannot create epoll fd\n");

	// waiting for digest command
	epev[0].data.fd = g_fs_log->digest_fd[0];
	epev[0].events = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
	ret = epoll_ctl(epfd, EPOLL_CTL_ADD, 
			g_fs_log->digest_fd[0], &epev[0]);
	if (ret < 0)
		panic("fail to connect epoll fd\n");

	// waiting for kernfs message
	epev[1].data.fd = g_sock_fd;
	epev[1].events = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
	ret = epoll_ctl(epfd, EPOLL_CTL_ADD, g_sock_fd, &epev[1]);
	if (ret < 0)
		panic("fail to connect epoll fd\n");

	*((int*)arg) = 1;

	mlfs_debug("%s\n", "digest thread starts");

	while(1) {
		int i;
		n = epoll_wait(epfd, epev, EVENT_COUNT, -1);

		if (n < 0 && errno != EINTR)
			panic("epoll wait problem: digest completion\n");

		for (i = 0; i < n; i++) {
			int _fd = epev[i].data.fd;
			socklen_t len = sizeof(struct sockaddr_un);

			if (_fd == g_fs_log->digest_fd[0]) {
				mlfs_debug("digest_pipe: event %d\n", epev[i].events);
				ret = read(_fd, cmd_buf, MAX_CMD_BUF);
				if (ret == 0)
					continue;

				mlfs_info("[D] cmd: %s\n", cmd_buf);

				// digest command
				if (cmd_buf[1] == 'd') {
					char cmd_header[10];
					int percent, j;
					uint32_t n_digest, digest_count = 0;
					addr_t loghdr_blkno;
					lru_node_t *node, *tmp;
					offset_t key;

					sscanf(cmd_buf, "|%s |%d|", cmd_header, &percent);
					n_digest = make_digest_request_sync(percent);

					loghdr_blkno = g_log_sb->start_digest;

#if 0
					// This has performance impact. I leave the code to improve later.
					// Build invalidate list before waiting ack from kernel FS.
					while (1) {
						loghdr_t *loghdr;
						struct fcache_block *fc_block;
						struct inode *inode;

						//mlfs_debug("read log header %lx\n", loghdr_blkno);
						loghdr = read_log_header(g_fs_log->dev, loghdr_blkno);

						if (loghdr->inuse != LH_COMMIT_MAGIC) {
							mlfs_printf("%s\n", "warning: early finish"
									" by meeting uncommited block\n");
							break;
						}

						for (j = 0; j < loghdr->n; j++) {
							if (loghdr->type[j] == L_TYPE_FILE) {
								//For inode resync from g_root_dev after digesting is done.
								inode = icache_find(g_root_dev, loghdr->inode_no[j]);
								mlfs_assert(inode);
								inode->flags |= I_RESYNC;

								key = (loghdr->data[j] >> g_block_size_shift);

								fc_block = fcache_find(inode, key);

								// the log block will be reclaimed so mark it as invalidated entry.
								if (fc_block)
									fc_block->invalidate = 1;

							}
						}

						digest_count++;
						loghdr_blkno = loghdr->next_loghdr_blkno;

						if (digest_count == n_digest)
							break;
					}
#endif
					// Waiting for ACK of digest from kernfs.
					ret = epoll_wait(kernfs_epfd, &kernfs_epev, 1, -1); 
					if (ret >= 0) {
						ret = recvfrom(g_sock_fd, buf, MAX_SOCK_BUF, 0, 
								(struct sockaddr *)&srv_addr, &len);

						mlfs_info("received %s\n", buf);

						handle_digest_response(buf);
					} 
				}
			} else if (_fd == g_sock_fd) {
				mlfs_debug("kernfs: event %d\n", epev[i].events);
				ret = recvfrom(g_sock_fd, buf, MAX_SOCK_BUF, 0, 
						(struct sockaddr *)&srv_addr, &len);
				if (ret == 0)
					continue;

				mlfs_debug("kernfs cmd: %s\n", buf);
			}
		} 
	}
}
