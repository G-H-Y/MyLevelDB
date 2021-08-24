//
// Created by hgong on 7/6/2021.
//
#include <atomic>
#include <map>
#include <queue>
#include <thread>
#include <sys/time.h>

#include "leveldb/env.h"
#include "leveldb/status.h"

#include "port/port.h"
#include "util/mutexlock.h"

#include "plog/include/plog_fabric.h"

namespace leveldb {

const char *esn = "HS00027AYF10K9000095";
const char *bdf = "0000:d9:00.0";
#define PLOG_ID_LENGTH 8
#define COMPLETION_USLEEP_TIME 1000
#define CTX_ISALLDONE_TIMEOUT 12
#define PLOG_CTX_TIMEOUT_MS (12 * 1000)
#define PLOG_CREATE_SIZE 1024
#define PLOG_RW_MIN_GRLTY 16 /* TODO : 盘接入时具体值从盘获取保存到NS表中，当前先固定值 */
#define PLOG_PAGE_SIZE 4096
#define PLOG_DIF_AREA_SPACE 64
#define PLOG_SEAL_REASON 3
#define ENTRY_PER_SGL 64

enum plog_id_cvs {
  PLOG_LDB,
  PLOG_MFST,
  PLOG_LOG,
  PLOG_LOCK,
  PLOG_CUR,
  PLOG_OTHER,
};

enum plog_done {
  PLOG_FABRIC_ONGOING,
  PLOG_FABRIC_DONE,
};

enum plog_status {
  PLOG_FABRIC_STATUS_SUCCESS,
  PLOG_FABRIC_STATUS_TIMEOUT,
  PLOG_FABRIC_STATUS_FAILED,
};

struct plog_rw_status {
  enum plog_status status;
  enum plog_done done;
  int status_code;
  int code_type;
};

static inline void plog_status_init(struct plog_rw_status *status){
  status->done = PLOG_FABRIC_ONGOING;
  status->status = PLOG_FABRIC_STATUS_TIMEOUT;
}

static inline int is_plog_cmd_done(struct plog_rw_status *status){
  return status->done == PLOG_FABRIC_DONE;
}

static inline int is_plog_status_success(struct plog_rw_status *status){
  return status->status == PLOG_FABRIC_STATUS_SUCCESS;
}


void plog_rw_done(int status_code, int code_type, void *cb_arg){
  struct plog_rw_status *status = (struct plog_rw_status *)cb_arg;
  status->status_code = status_code;
  status->code_type = code_type;
  if (status_code == 0 && code_type == 0) {
    status->status = PLOG_FABRIC_STATUS_SUCCESS;
  } else {
    status->status = PLOG_FABRIC_STATUS_FAILED;
  }
  status->done = PLOG_FABRIC_DONE;
}

static int init_plog_disk(struct plog_disk *pdisk, const char *pesn, const char *pbdf, uint32_t nsid)
{
  strcpy(pdisk->esn, pesn);
  strcpy(pdisk->trsvcid, pbdf);
  strcpy(pdisk->traddr, PLOG_INVALID_IP);
  pdisk->nsid = nsid;
  return 0;
}

static int init_plog_io_ctx(struct plog_io_ctx *ctx, plog_usercb_func cb, void *arg)
{
  ctx->cb = cb;
  ctx->cb_arg = arg;
  ctx->timeout_ms = PLOG_CTX_TIMEOUT_MS;
  return 0;
}

static inline uint32_t plog_calc_rw_len(uint32_t data_len, uint16_t gran){
  if (data_len == 0) {
    return 0;
  }
  return ((data_len + gran - 1) / gran - 1);  /* 0 base value */
}

class PlogRWStatusPool{
 public:
  PlogRWStatusPool(){};
  ~PlogRWStatusPool(){
    for(auto it=plog_rw_status_pool_.begin(); it!=plog_rw_status_pool_.end(); it++){
      delete it->second;
    }
    plog_rw_status_pool_.clear();
  }

  struct plog_rw_status* Allocator(std::string filename){
    MutexLock lock(&pool_mutex_); //std::mutex.lock() blocks if the mutex is not available
    struct plog_rw_status* p_status = new plog_rw_status();
    plog_status_init(p_status);
    plog_rw_status_pool_[filename] = p_status;
    return p_status;
  }

  Status Deallocator(std::string filename, struct plog_rw_status* p_status){
    MutexLock lock(&pool_mutex_);
    if(plog_rw_status_pool_.find(filename) == plog_rw_status_pool_.end()){
      printf("Deallocator Status not found!\n");
    }else if(plog_rw_status_pool_[filename]!= p_status){
      printf("Deallocator Status address changed!\n");
    }else{
      while (!is_plog_cmd_done(p_status)) {
        usleep(COMPLETION_USLEEP_TIME);
      }
      if (!is_plog_status_success(p_status)) {
        return Status::IOError(filename, "plog write failed");
      }
      plog_rw_status_pool_.erase(filename);
      //delete p_status;
      //p_status = NULL;
    }
    return Status::OK();
  }

  bool isAllDone(){
    MutexLock lock(&pool_mutex_);
    int cnt = 0;
    bool done = true;
    while(true){
      for(auto it=plog_rw_status_pool_.begin(); it!=plog_rw_status_pool_.end(); it++){
        struct plog_rw_status* p_status = it->second;
        while(!is_plog_cmd_done(p_status)){
          usleep(COMPLETION_USLEEP_TIME);
        }
        if(!is_plog_status_success(p_status)){
          done = false;
        }
      }
      if(done) return true;
      cnt++;
      if(cnt>CTX_ISALLDONE_TIMEOUT) return false;
    }
  }

 private:
  port::Mutex pool_mutex_;
  std::map<std::string, struct plog_rw_status*> plog_rw_status_pool_ GUARDED_BY(pool_mutex_);
};

class PlogFile{
 public:
  const std::string filename_;
  const uint64_t plog_id_;
  bool flag_;
  bool is_current_;
  bool is_manifest_;
  bool do_delete_;
  bool is_sealed_;
  struct plog_rw_status *p_status_;
  static PlogRWStatusPool* write_status_pool_;
  static int plog_disk_fd_;
  static port::Mutex files_mutex_;
  static std::map<std::string,PlogFile*> files_ GUARDED_BY(files_mutex_);

  PlogFile(const std::string filename)
      : filename_(filename),
        plog_id_(NametoID(filename)),
        p_status_(nullptr),
        flag_(true),
        do_delete_(false),
        is_sealed_(false),
        is_manifest_(isManifest(filename)),
        is_current_(isCurrent(filename)),
        refs_(0){}
  ~PlogFile(){};

  static bool isManifest(const std::string filename){
    return Slice(filename).starts_with("MANIFEST");
  }

  static bool isCurrent(const std::string filename){
    return Slice(filename).starts_with("CURRENT");
  }

  static bool isLOG(const std::string filename){
    return Slice(filename).starts_with("LOG");
  }

  static bool isLOCK(const std::string filename){
    return Slice(filename).starts_with("LOCK");
  }

  static bool isLDB(const std::string filename){
    std::size_t found = filename.find('.');
    if(found != std::string::npos){
      std::string substr = filename.substr(found,filename.length());
      return Slice(substr).starts_with(".ldb");
    }
    return false;
  }

  //TODO():std::hash
  static uint64_t NametoID(std::string filename){
    uint64_t plog_id;
    int found = filename.find('.');
    int filename_end;
    std::string filename_cut;
    if(found != std::string::npos){
      filename_end = found > PLOG_ID_LENGTH ? PLOG_ID_LENGTH-1 : found;
    }else{
      filename_end = filename.size() > PLOG_ID_LENGTH ? PLOG_ID_LENGTH-1 :filename.size();
    }

    if(isLDB(filename)){
      filename_cut = filename.substr(0,filename_end) + std::to_string(PLOG_LDB);
    }else if(isCurrent(filename)){
      filename_cut = filename + std::to_string(PLOG_CUR);
    }else if(isManifest(filename)){
      filename_cut = filename + std::to_string(PLOG_MFST);
    }else if(isLOG(filename)){
      filename_cut = filename + std::to_string(PLOG_LOG);
    }else if(isLOCK(filename)){
      filename_cut = filename + std::to_string(PLOG_LOCK);
    }else{
      filename_cut = filename.substr(0,filename_end) + std::to_string(PLOG_OTHER);
    }
    char* pend;
    plog_id = std::strtoull(filename_cut.data(), &pend, 0);
    if(pend == filename_cut.data()){
      plog_id = *(uint64_t*) filename_cut.data();
    }
    return plog_id;
  }

  static uint64_t GetSize(std::string filename, uint64_t plog_id){
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_attr_table attr = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = plog_id;
    plog_get_attr(plog_disk_fd_,&plog_param,&attr,&ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return -1;
    }
    return attr.written_size;
  }

  static Status DoCreate(std::string filename, uint64_t plog_id){
    if(plog_disk_fd_ < 1){
      return Status::IOError(filename,"plog_disk_fd_ < 1");
    }
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_attr_table attr = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = plog_id;
    memset(&attr, 0x0, sizeof(attr));
    attr.create_size = PLOG_CREATE_SIZE;
    plog_create(plog_disk_fd_,&plog_param,&attr,&ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError(filename,"Create Failed");
    }
    return Status::OK();
  }

  static Status DoAppend(std::string filename, uint64_t plog_id, const Slice& sdata, struct plog_rw_status* plog_status){
    if(plog_disk_fd_ < 1){
      return Status::IOError(filename,"plog_disk_fd_ < 1");
    }
    struct plog_io_ctx ctx = {0};
    struct plog_rw_param rw_info = {static_cast<plog_rw_opt>(0)};
    plog_drv_buf_list_t plog_buf = {0};
    void* data = NULL;
    uint64_t offset;
    uint64_t len;
    uint64_t dif_len = PLOG_DIF_AREA_SPACE;
    uint64_t aligned_offset;
    uint64_t aligned_len;
    bool is_unaligned;
    int bufs;

    init_plog_io_ctx(&ctx, plog_rw_done, plog_status);
    plog_status_init(plog_status);

    uint64_t written_size = GetSize(filename,plog_id);
    if(written_size < 0){
      return Status::IOError(filename, "written size = -1");
    }
    offset = written_size;
    len = sdata.size();
    rw_info.offset = offset / PLOG_RW_MIN_GRLTY;
    rw_info.length = plog_calc_rw_len(len, PLOG_RW_MIN_GRLTY);
    rw_info.plog_param.plog_id = plog_id;
    rw_info.plog_param.access_id = 0;
    rw_info.plog_param.pg_version = 0;
    rw_info.opt = PLOG_RW_OPT_APPEND;

    aligned_offset = rw_info.offset*PLOG_RW_MIN_GRLTY;
    aligned_len = (rw_info.length+1)*PLOG_RW_MIN_GRLTY;
    is_unaligned = (aligned_offset % PLOG_PAGE_SIZE);
    uint32_t curpage_rmn_len = PLOG_PAGE_SIZE - (aligned_offset % PLOG_PAGE_SIZE);
    uint32_t page_rmn_len = is_unaligned ? (aligned_len>curpage_rmn_len ? curpage_rmn_len : aligned_len) : 0;
    uint32_t data_rmn_len = aligned_len - page_rmn_len;
    bufs = (is_unaligned) + (data_rmn_len/PLOG_PAGE_SIZE) + (data_rmn_len%PLOG_PAGE_SIZE?1:0);

    data = rte_malloc(NULL, aligned_len + dif_len*bufs, PLOG_RW_MIN_GRLTY);
    if (data == NULL) {
      return Status::IOError(filename, "calloc bufflists failed");
    }
    plog_buf.buffers = (plog_drv_buf_t *)calloc(bufs, sizeof(plog_drv_buf_t));
    if (plog_buf.buffers == NULL) {
      return Status::IOError(filename, "calloc bufflists failed");
    }

    plog_buf.cnt = bufs;
    uint64_t remain = aligned_len;
    char* src = const_cast<char*>(sdata.data());
    for(int i=0; i<bufs && remain>0; i++){
      if(i==0 && is_unaligned){
        std::memcpy(data,sdata.data(),page_rmn_len);
        plog_buf.buffers[0].buf = (char*)data;
        plog_buf.buffers[0].len = page_rmn_len + dif_len;
        remain = aligned_len - page_rmn_len;
      }else{
        char* bstart = (char*)data + (page_rmn_len+dif_len)*is_unaligned + (PLOG_PAGE_SIZE+dif_len)*(i-is_unaligned);
        plog_buf.buffers[i].buf = bstart;
        if(remain >= PLOG_PAGE_SIZE){
          std::memcpy(bstart,src+(aligned_len-remain),PLOG_PAGE_SIZE);
          plog_buf.buffers[i].len = PLOG_PAGE_SIZE + dif_len;
          remain -= PLOG_PAGE_SIZE;
        }else{
          std::memcpy(bstart,src+(aligned_len-remain),remain);
          plog_buf.buffers[i].len = remain + dif_len;
          remain  = 0;
        }
      }
    }

    plog_append_buff(plog_disk_fd_, &rw_info, &plog_buf, NULL, &ctx);
    while (!is_plog_cmd_done(plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    write_status_pool_->Deallocator(filename,plog_status);
    if (!is_plog_status_success(plog_status)) {
      return Status::IOError(filename, "plog write failed");
    }

    rte_free(data);
    free(plog_buf.buffers);
    return Status::OK();
  }

  static Status DoAppend1(std::string filename, uint64_t plog_id, uint64_t len,plog_drv_buf_list_t& plog_buf){
    if(plog_disk_fd_ < 1){
      return Status::IOError(filename,"plog_disk_fd_ < 1");
    }
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_rw_param rw_info = {static_cast<plog_rw_opt>(0)};

    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);

    rw_info.offset = 0 / PLOG_RW_MIN_GRLTY;
    rw_info.length = plog_calc_rw_len(len, PLOG_RW_MIN_GRLTY);
    rw_info.plog_param.plog_id = plog_id;
    rw_info.plog_param.access_id = 0;
    rw_info.plog_param.pg_version = 0;
    rw_info.opt = PLOG_RW_OPT_APPEND;
    plog_append_buff(plog_disk_fd_, &rw_info, &plog_buf, NULL, &ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError(filename, "plog write failed");
    }

    return Status::OK();
  }

  static Status DoRead(std::string filename, uint64_t plog_id, uint64_t offset, size_t len, Slice* result, char* scratch){
    if(plog_disk_fd_ < 1){
      return Status::IOError(filename,"plog_disk_fd_ < 1");
    }
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_rw_param rw_info = {static_cast<plog_rw_opt>(0)};
    plog_drv_buf_list_t plog_buf;
    void* data = NULL;
    uint64_t dif_len = PLOG_DIF_AREA_SPACE;
    uint64_t aligned_offset;
    uint64_t aligned_len;
    bool is_unaligned; //unaligned with 4096
    int unaligned16; //unaligned with 16
    int bufs;
    uint64_t plog_size = GetSize(filename,plog_id);

    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);

    rw_info.offset = offset / PLOG_RW_MIN_GRLTY;
    rw_info.length = plog_calc_rw_len(len, PLOG_RW_MIN_GRLTY);
    unaligned16 = offset%16;
    if(unaligned16 && (unaligned16+len)>16){ rw_info.length++; }
    aligned_offset = rw_info.offset*PLOG_RW_MIN_GRLTY;
    aligned_len = (rw_info.length+1)*PLOG_RW_MIN_GRLTY;
    if((aligned_offset+aligned_len) > plog_size){
      rw_info.length = plog_calc_rw_len(plog_size-aligned_offset,PLOG_RW_MIN_GRLTY);
    }
    rw_info.plog_param.plog_id = plog_id;
    rw_info.plog_param.access_id = 0;
    rw_info.plog_param.pg_version = 0;
    rw_info.opt = PLOG_RW_OPT_READ;

    aligned_len = (rw_info.length+1)*PLOG_RW_MIN_GRLTY;
    is_unaligned = (aligned_offset % PLOG_PAGE_SIZE);
    uint32_t curpage_rmn_len = PLOG_PAGE_SIZE - (aligned_offset % PLOG_PAGE_SIZE);
    uint32_t page_rmn_len = is_unaligned ? (aligned_len>curpage_rmn_len ? curpage_rmn_len : aligned_len) : 0;
    uint32_t data_rmn_len = aligned_len - page_rmn_len;
    bufs = (is_unaligned) + (data_rmn_len/PLOG_PAGE_SIZE) + (data_rmn_len%PLOG_PAGE_SIZE?1:0);

    plog_buf.buffers = (plog_drv_buf_t *)calloc(bufs, sizeof(plog_drv_buf_t));
    if (plog_buf.buffers == NULL) {
      return Status::IOError(filename, "calloc bufflists failed");
    }
    data = rte_malloc(NULL, aligned_len + dif_len*bufs, PLOG_RW_MIN_GRLTY);
    if (data == NULL) {
      return Status::IOError(filename, "calloc bufflists failed");
    }

    plog_buf.cnt = bufs;
    uint64_t remain = aligned_len;
    for(int i=0; i<bufs && remain>0; i++){
      if(i==0 && is_unaligned){
        plog_buf.buffers[0].buf = (char*)data;
        plog_buf.buffers[0].len = page_rmn_len + dif_len;
        remain = aligned_len - page_rmn_len;
      }else{
        char* bstart = (char*)data + (page_rmn_len+dif_len)*is_unaligned + (PLOG_PAGE_SIZE+dif_len)*(i-is_unaligned);
        plog_buf.buffers[i].buf = bstart;
        if(remain >= PLOG_PAGE_SIZE){
          plog_buf.buffers[i].len = PLOG_PAGE_SIZE + dif_len;
          remain -= PLOG_PAGE_SIZE;
        }else{
          plog_buf.buffers[i].len = remain + dif_len;
          remain  = 0;
        }
      }
    }

    plog_read_buff(plog_disk_fd_, &rw_info, &plog_buf, NULL, &ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError(filename, "plog read failed");
    }

    scratch = (char*)rte_malloc(NULL, aligned_len, PLOG_RW_MIN_GRLTY);
    if (scratch == NULL) {
      return Status::IOError(filename, "malloc scratch failed");
    }

    remain = aligned_len;
    for(int i=0; i<bufs && remain>0; i++){
      if(i==0 && is_unaligned){
        std::memcpy(scratch, plog_buf.buffers[0].buf, page_rmn_len);
        remain = aligned_len - page_rmn_len;
      }else{
        uint64_t soffset = (page_rmn_len)*is_unaligned + (PLOG_PAGE_SIZE)*(i-is_unaligned);
        if(remain >= PLOG_PAGE_SIZE){
          std::memcpy(scratch+soffset, plog_buf.buffers[i].buf, PLOG_PAGE_SIZE);
          remain -= PLOG_PAGE_SIZE;
        }else{
          std::memcpy(scratch+soffset,  plog_buf.buffers[i].buf, remain);
          remain  = 0;
        }
      }
    }
    *result = Slice(scratch+unaligned16,len);

    rte_free(data);
    free(plog_buf.buffers);
    return Status::OK();
  }

  static Status DoRead1(std::string filename, uint64_t plog_id, uint64_t len, plog_drv_buf_list_t &plog_buf){
    if(plog_disk_fd_ < 1){
      return Status::IOError(filename,"plog_disk_fd_ < 1");
    }
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_rw_param rw_info = {static_cast<plog_rw_opt>(0)};
    void* data = NULL;
    uint64_t dif_len = PLOG_DIF_AREA_SPACE;
    int bufs;

    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);

    rw_info.offset = 0;
    rw_info.length = plog_calc_rw_len(len, PLOG_RW_MIN_GRLTY);
    rw_info.plog_param.plog_id = plog_id;
    rw_info.plog_param.access_id = 0;
    rw_info.plog_param.pg_version = 0;
    rw_info.opt = PLOG_RW_OPT_READ;

    bufs = (len/PLOG_PAGE_SIZE) + (len%PLOG_PAGE_SIZE?1:0);

    plog_buf.buffers = (plog_drv_buf_t *)calloc(bufs, sizeof(plog_drv_buf_t));
    if (plog_buf.buffers == NULL) {
      return Status::IOError(filename, "calloc bufflists failed");
    }
    data = rte_malloc(NULL, len + dif_len*bufs, PLOG_RW_MIN_GRLTY);
    if (data == NULL) {
      return Status::IOError(filename, "calloc bufflists failed");
    }

    plog_buf.cnt = bufs;
    uint64_t remain = len;
    for(int i=0; i<bufs && remain>0; i++){
      char* bstart = (char*)data + (PLOG_PAGE_SIZE+dif_len)*i;
      plog_buf.buffers[i].buf = bstart;
      if(remain >= PLOG_PAGE_SIZE){
        plog_buf.buffers[i].len = PLOG_PAGE_SIZE + dif_len;
        remain -= PLOG_PAGE_SIZE;
      }else{
        plog_buf.buffers[i].len = remain + dif_len;
        remain  = 0;
      }
    }

    plog_read_buff(plog_disk_fd_, &rw_info, &plog_buf, NULL, &ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError(filename, "plog read failed");
    }

    return Status::OK();
  }

  static Status DoRemove(std::string filename, uint64_t plog_id){
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = plog_id;
    plog_delete(PlogFile::plog_disk_fd_,&plog_param,&ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(PlogFile::plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError(filename,"RemoveFile Failed");
    }
    return Status::OK();
  }

  static Status DoSeal(std::string filename, uint64_t plog_id){
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = plog_id;
    uint64_t seal_size = GetSize(filename, plog_id);
    if(seal_size < 0){
      return Status::IOError("Close " + filename, "seal_size = -1");
    }
    plog_seal(plog_disk_fd_,&plog_param,seal_size,PLOG_SEAL_REASON,&ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError("Close " + filename,"seal failed");
    }
    return Status::OK();
  }

  static Status CopyPlog(const std::string src, uint64_t src_id, const std::string dst, uint64_t dst_id, bool is_src_sealed){
    //get size of src plog
    uint64_t src_len = GetSize(src,src_id);
    if(src_len == -1){
      return Status::IOError(src,"Size = -1");
    }
    plog_drv_buf_list_t plog_buf;
    Status s;
    s = DoRead1(src, src_id, src_len, plog_buf);
    if(!s.ok()){
      return Status::IOError(src,"read failed");
    }
    s = DoCreate(dst,dst_id);
    if(!s.ok()){
      return Status::IOError(src,"create dst failed while doing copy");
    }

    s = DoAppend1(dst, dst_id, src_len, plog_buf);
    if(!s.ok()){
      DoSeal(dst,dst_id);
      DoRemove(dst, dst_id);
      return Status::IOError(dst, "Append dst failed");
    }

    s = DoSeal(dst,dst_id);
    if(!s.ok()){
      return Status::IOError(dst, "Seal dst falied");
    }
    if(!is_src_sealed){
      s = DoSeal(src, src_id);
    }
    s = DoRemove(src,src_id);
    if(!s.ok()){
      return Status::IOError(src, "Delete src falied");
    }
    return Status::OK();
  }

  void Ref(){
    MutexLock lock(&refs_mutex_);
    ++refs_;
  }

  void Unref(){
    MutexLock lock(&refs_mutex_);
    --refs_;
    assert(refs_>=0);
    if(refs_<=0 && do_delete_){
      MutexLock flock(&files_mutex_);
      files_.erase(filename_);
      DoRemove(filename_,plog_id_);
      delete this;
    }
  }

  uint64_t Size(){
    return GetSize(filename_,plog_id_);
  }

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const{
    return DoRead(filename_, plog_id_, offset, n, result, scratch);
  }

  //must be called at first when NewWritableFile
  Status Create(){
    if(plog_disk_fd_ < 1){
      return Status::IOError(filename_,"plog_disk_fd_ < 1");
    }
    Status s = DoCreate(filename_,plog_id_);
    if(!s.ok()){
      flag_ = false;
      return Status::IOError(filename_,"plog_disk_fd_ < 1");
    }
    flag_ = true;
    return Status::OK();
  }

  Status Append(const Slice& data){
    if(plog_disk_fd_ < 1){
      return Status::IOError(filename_,"plog_disk_fd_ < 1");
    }
    if(!flag_ && !is_manifest_){
      return Status::IOError(filename_,"flag_ is false");
    }
    if(p_status_== nullptr){
      p_status_ = write_status_pool_->Allocator(filename_);
    }
    return DoAppend(filename_, plog_id_, data, p_status_);
  }

  Status Remove(){
    MutexLock lock(&refs_mutex_);
    do_delete_ = true;
    if(refs_<=0){
      Status s = DoSeal(filename_, plog_id_);
      is_sealed_ = true;
      if(!s.ok()){
        return Status::IOError(filename_,"Seal failed");
      }
      s = DoRemove(filename_,plog_id_);
      if(!s.ok()){
        return Status::IOError(filename_,"Remove failed");
      }
    }
    return Status::OK();
  }

  Status Close(){
    Status s;
    if(plog_disk_fd_ < 1){
      return Status::IOError("Close " + filename_,"plog_disk_fd_ < 1");
    }
    delete p_status_;
    p_status_ = NULL;
    Unref();
    return s;
  }

 private:
  port::Mutex refs_mutex_;
  int refs_ GUARDED_BY(refs_mutex_);
};

int PlogFile::plog_disk_fd_ = 0;
PlogRWStatusPool* PlogFile::write_status_pool_ = nullptr;
port::Mutex PlogFile::files_mutex_;
std::map<std::string,PlogFile*> PlogFile::files_ GUARDED_BY(PlogFile::files_mutex_);

class PlogSequentialFile final : public SequentialFile{
 public:
  PlogSequentialFile(PlogFile* plog_file)
      : plog_file_(plog_file), pos_(0) { plog_file_->Ref();}

  Status Read(size_t n, Slice* result, char* scratch) override{
    if(plog_file_->is_current_){
      //TODO(): go to zookeeper
      return Status::OK();
    }else{
      Status s = plog_file_->Read(pos_,n,result,scratch);
      if(s.ok()){
        pos_ += result->size();
      }
      return s;
    }
  }

  Status Skip(uint64_t n) override{
    if(pos_ > plog_file_->Size()){
      return Status::IOError(plog_file_->filename_, "pos_ > plog_file->Size()");
    }
    const uint64_t available = plog_file_->Size() - pos_;
    if(n>available){
      n = available;
    }
    pos_ += n;
    return Status::OK();
  }
 private:
  ~PlogSequentialFile() override { plog_file_->Unref();}
  PlogFile *plog_file_;
  uint64_t pos_;
};

class PlogRandomAccessFile final : public RandomAccessFile{
 public:
  PlogRandomAccessFile(PlogFile* plog_file): plog_file_(plog_file) { plog_file_->Ref();}

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override{
    return plog_file_->Read(offset,n,result,scratch);
  }

 private:
  ~PlogRandomAccessFile() override { plog_file_->Unref();}
  PlogFile *plog_file_;
};

class PlogWritableFile final :  public WritableFile{
 public:
  PlogWritableFile(PlogFile *plog_file)
      : plog_file_(plog_file), pos_(0) { plog_file_->Ref();}

  Status Append(const Slice& data) override{
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, PLOG_PAGE_SIZE - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.
    if (write_size < PLOG_PAGE_SIZE) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);

  }

  Status Close() override{
    Status s = FlushBuffer();
    if(s.ok()){
      s = plog_file_->Close();
    }
    plog_file_ = NULL;
    return s;
  }

  Status Flush() override{
    return FlushBuffer();
  }
  Status Sync() override{
    //if is manefist should wait until all p_status is done
    if(plog_file_->is_manifest_){
      if(PlogFile::write_status_pool_->isAllDone()){
        return FlushBuffer();
      }else{
        return Status::IOError(plog_file_->filename_, "Sync MANIFEST: Other files haven't write to plog");
      }
    }else{
      return FlushBuffer();
    }
  }

 private:

  ~PlogWritableFile() override{
    if(plog_file_!= nullptr){
      Close();
    }
  }

  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  Status WriteUnbuffered(const char* data, size_t size) {
    Status s = plog_file_->Append(Slice(data,size));
    return s;
  }

  char buf_[PLOG_PAGE_SIZE];
  size_t pos_;
  PlogFile* plog_file_;
};

class PlogEnv : public Env{
 public:
  struct plog_disk pdisk_;
  struct plog_io_ctx ctx_;
  struct plog_rw_status plog_status_;

  PlogEnv():background_work_cv_(&background_work_mutex_),
            started_background_thread_(false){
    plog_fabric_init();
    init_plog_disk(&pdisk_, esn, bdf, PLOG_DEFAULT_NS);
    init_plog_io_ctx(&ctx_, plog_rw_done, &plog_status_);
    plog_status_init(&plog_status_);
    plog_create_ctrlr(&pdisk_, &ctx_);
    while (!is_plog_cmd_done(&plog_status_)) {
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (is_plog_status_success(&plog_status_)) {
      int fd = plog_open(&pdisk_);
      PlogFile::plog_disk_fd_ = fd;
      PlogFile::write_status_pool_ = new PlogRWStatusPool() ;
    }
  }
  ~PlogEnv() override {
    delete PlogFile::write_status_pool_;
    int fd = PlogFile::plog_disk_fd_;
    if(fd>0){
      plog_close(fd);
      plog_delete_ctrlr(&pdisk_);
      plog_fabric_fini();
    }
  }

  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    MutexLock lock(&PlogFile::files_mutex_);
    PlogFile* plog_file;
    if(PlogFile::files_.find(filename)!=PlogFile::files_.end()){
      plog_file = PlogFile::files_[filename];
    }else{
      return Status::IOError(filename, "not exist in PlogFile::files_ at all");
    }
    if(plog_file->Size() == -1){
      return Status::IOError(filename, "plog size = -1");
    }
    *result = new PlogSequentialFile(plog_file);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    MutexLock lock(&PlogFile::files_mutex_);
    PlogFile* plog_file;
    if(PlogFile::files_.find(filename)!=PlogFile::files_.end()){
      plog_file = PlogFile::files_[filename];
    }else{
      return Status::IOError(filename, "not exist in PlogFile::files_ at all");
    }
    if(plog_file->Size() == -1){
      return Status::IOError(filename, "plog size = -1");
    }
    *result = new PlogRandomAccessFile(plog_file);
    return Status::OK();
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    MutexLock lock(&PlogFile::files_mutex_);
    PlogFile *plog_file;
    if(PlogFile::files_.find(filename)!=PlogFile::files_.end()){
      plog_file = PlogFile::files_[filename];
    }else{
      plog_file = new PlogFile(filename);
      Status s = plog_file->Create();
      if(!s.ok()){
        return Status::IOError(filename, "create writablefile failed");
      }
      PlogFile::files_[filename] = plog_file;
    }
    *result = new PlogWritableFile(plog_file);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    MutexLock lock(&PlogFile::files_mutex_);
    PlogFile *plog_file;
    if(PlogFile::files_.find(filename)!=PlogFile::files_.end()){
      plog_file = PlogFile::files_[filename];
    }else{
      plog_file = new PlogFile(filename);
      Status s = plog_file->Create();
      if(!s.ok()){
        return Status::IOError(filename, "create appendablefile failed");
      }
      PlogFile::files_[filename] = plog_file;
    }
    *result = new PlogWritableFile(plog_file);
    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    //find dbname/CURRENT
    //TODO():go to zookeeper to see if CURRENT file exists
    return true;
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {

    return Status::OK();
  }

  Status RemoveFile(const std::string& filename) override {
    Status s;
    MutexLock lock(&PlogFile::files_mutex_);
    if(PlogFile::files_.find(filename)!=PlogFile::files_.end()){
      s = PlogFile::files_[filename]->Remove();
    }else{
      return Status::IOError(filename,"File has already been removed from PlogFile::files_");
    }
    return s;
  }

  Status CreateDir(const std::string& dirname) override {

    return Status::OK();
  }

  Status RemoveDir(const std::string& dirname) override {

    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    PlogFile *plog_file = new PlogFile(filename);
    *size = plog_file->Size();
    if(*size == -1){
      return Status::IOError(filename,"Size is -1");
    }
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    if(PlogFile::isCurrent(from)){
      //TODO():go to zookeeper
    }else{
      MutexLock lock(&PlogFile::files_mutex_);
      PlogFile* plog_file;
      if(PlogFile::files_.find(from)!=PlogFile::files_.end()){
        plog_file = PlogFile::files_[from];
      }else{
        return Status::IOError(from, "not exist in PlogFile::files_ at all");
      }
      if(plog_file->Size() == -1){
        return Status::IOError(from, "plog size = -1");
      }
      uint64_t from_id = PlogFile::NametoID(from);
      uint64_t to_id = PlogFile::NametoID(to);
      bool is_src_sealed = plog_file->is_sealed_;
      Status s = PlogFile::CopyPlog(from,from_id,to,to_id,is_src_sealed);
      if(!s.ok()){
        return Status::IOError(from, "rename file failed");
      }
      plog_file = new PlogFile(to);
      PlogFile::files_[to] = plog_file;
      PlogFile::files_.erase(from);
    }
    return Status::OK();
  }

  Status LockFile(const std::string& filename, FileLock** lock) override {

    return Status::OK();
  }

  Status UnlockFile(FileLock* lock) override {

    return Status::OK();
  }

  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;

  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override {
    std::thread new_thread(thread_main, thread_main_arg);
    new_thread.detach();
  }

  Status GetTestDirectory(std::string* result) override {
    *result = "";
    return Status::OK();
  }

  Status NewLogger(const std::string& filename, Logger** result) override {

    return Status::OK();
  }

  uint64_t NowMicros() override {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
  }

  void SleepForMicroseconds(int micros) override {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
  }

 private:
  void BackgroundThreadMain();

  static void BackgroundThreadEntryPoint(PlogEnv* env) {
    env->BackgroundThreadMain();
  }

  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (*const function)(void*);
    void* const arg;
  };

  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  std::queue<BackgroundWorkItem> background_work_queue_
  GUARDED_BY(background_work_mutex_);

};

void PlogEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock();

  // Start the background thread, if we haven't done so already.
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PlogEnv::BackgroundThreadEntryPoint, this);
    background_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }

  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}

void PlogEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();

    // Wait until there is work to be done.
    while (background_work_queue_.empty()) {
      background_work_cv_.Wait();
    }

    assert(!background_work_queue_.empty());
    auto background_work_function = background_work_queue_.front().function;
    void* background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    background_work_function(background_work_arg);
  }
}

namespace {
template <typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order::memory_order_relaxed);
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType();
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template <typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PlogDefaultEnv = SingletonEnv<PlogEnv>;

}  // namespace

Env* Env::Default() {
  static PlogDefaultEnv env_container;
  return env_container.env();
}
}