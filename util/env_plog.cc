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
constexpr const size_t kWritableFileBufferSize = 65536;
const char *esn = "HS00027AYF10K9000095";
const char *bdf = "0000:d9:00.0";
#define COMPLETION_USLEEP_TIME 1000
#define CTX_ISALLDONE_TIMEOUT 12
#define PLOG_CTX_TIMEOUT_MS (12 * 1000)
#define PLOG_CREATE_SIZE 1024
#define PLOG_RW_MIN_GRLTY 16 /* TODO : 盘接入时具体值从盘获取保存到NS表中，当前先固定值 */
#define PLOG_PAGE_SIZE (kWritableFileBufferSize)
#define PLOG_DIF_AREA_SPACE 0 //TODO(): 64 or 0?
#define PLOG_SEAL_REASON 3
#define ENTRY_PER_SGL 64
#define RTE_ALIGN 16
//#define SGL_CNT 1

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

  struct plog_rw_status* Allocator(std::string plog_id){
    MutexLock lock(&pool_mutex_); //std::mutex.lock() blocks if the mutex is not available
    struct plog_rw_status* p_status = new plog_rw_status();
    plog_status_init(p_status);
    plog_rw_status_pool_[plog_id] = p_status;
    return p_status;
  }

  Status Deallocator(std::string plog_id, struct plog_rw_status* p_status){
    MutexLock lock(&pool_mutex_);
    if(plog_rw_status_pool_.find(plog_id) == plog_rw_status_pool_.end()){
      return Status::IOError(plog_id, "Plog Status not found");
    }else if(plog_rw_status_pool_[plog_id]!= p_status){
      return Status::IOError(plog_id, "Plog Status address changed");
    }else{
      while (!is_plog_cmd_done(p_status)) {
        usleep(COMPLETION_USLEEP_TIME);
      }
      if (!is_plog_status_success(p_status)) {
        return Status::IOError(plog_id, "plog write failed");
      }
      plog_rw_status_pool_.erase(plog_id);
      delete p_status;
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
  const std::string plog_id_;
  bool flag_;
  bool is_current_;
  bool is_manifest_;
  bool do_delete_;
  struct plog_rw_status *p_status_;
  static PlogRWStatusPool* write_status_pool_;
  static int plog_disk_fd_;
  static port::Mutex files_mutex_;
  static std::map<std::string,PlogFile*> files_ GUARDED_BY(files_mutex_);
  //static port::Mutex writefile_mutex_;
  //static std::map<std::string,PlogFile*> writefile_ GUARDED_BY(writefile_mutex_);

  PlogFile(const std::string plog_id)
      : plog_id_(plog_id),
        p_status_(nullptr),
        flag_(true),
        do_delete_(false),
        is_manifest_(isManifest(plog_id)),
        is_current_(isCurrent(plog_id)),
        refs_(0){}
  ~PlogFile(){};

  static bool isManifest(const std::string plog_id){
    return Slice(plog_id).starts_with("MANIFEST");
  }

  static bool isCurrent(const std::string plog_id){
    return Slice(plog_id).starts_with("CURRENT");
  }

  static uint64_t GetSize(std::string plog_id){
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_attr_table attr = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = std::strtoll(plog_id.data(), nullptr,0);
    memset(&attr, 0x0, sizeof(attr));
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

  static Status DoCreate(std::string plog_id){
    if(plog_disk_fd_ < 1){
      return Status::IOError(plog_id,"plog_disk_fd_ < 1");
    }
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_attr_table attr = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = std::strtoll(plog_id.data(), nullptr,0);
    memset(&attr, 0x0, sizeof(attr));
    attr.create_size = PLOG_CREATE_SIZE;
    plog_create(plog_disk_fd_,&plog_param,&attr,&ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      //flag_ = false;
      return Status::IOError(plog_id,"Create Failed");
    }
    //flag_ = true;
    return Status::OK();
  }

  static Status DoAppend(std::string plog_id, sgl_vector_t *sgl_vector,
                         struct plog_rw_status* plog_status, uint64_t src_len){
    if(plog_disk_fd_ < 1){
      return Status::IOError(plog_id,"plog_disk_fd_ < 1");
    }
    struct plog_io_ctx write_ctx = {0};
    struct plog_rw_param rw_info = {static_cast<plog_rw_opt>(0)};
    init_plog_io_ctx(&write_ctx, plog_rw_done, plog_status);
    plog_status_init(plog_status);
    uint64_t written_size = GetSize(plog_id);
    if(written_size < 0){
      return Status::IOError(plog_id, "written size = -1");
    }
    rw_info.offset = written_size / PLOG_RW_MIN_GRLTY;
    rw_info.length = plog_calc_rw_len(src_len,PLOG_RW_MIN_GRLTY);
    rw_info.plog_param.plog_id = std::strtoll(plog_id.data(), nullptr,0);
    rw_info.plog_param.access_id = 0;
    rw_info.plog_param.pg_version = 0;
    rw_info.opt = PLOG_RW_OPT_APPEND;
    plog_append_sgl(plog_disk_fd_, &rw_info, sgl_vector, &write_ctx);
    while (!is_plog_cmd_done(plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(plog_status)) {
      if(plog_status!= nullptr){
        write_status_pool_->Deallocator(plog_id,plog_status);
      }
      return Status::IOError(plog_id, "plog write failed");
    }
    //rte_free(data_rte);
    //free(sgl_vector);
    return Status::OK();
  }

  static Status DoRead(std::string plog_id, uint64_t offset, size_t n, Slice* result, char* scratch,
                       sgl_vector_t *sgl_vector, bool iscopy) {
    if(plog_disk_fd_ < 1){
      return Status::IOError(plog_id,"plog_disk_fd_ < 1");
    }
    struct plog_rw_status plog_status;
    struct plog_io_ctx read_ctx = {0};
    struct plog_rw_param rw_info = {static_cast<plog_rw_opt>(0)};
    //struct plog_sgl_s sgl = {0};
    //sgl_vector_t *sgl_vector = nullptr;
    //void *data = nullptr;
    //Each sgl_chain only has one sgl, each sgl has 64 entry(ENTRY_PER_SGL), each entry is PLOG_PAGE_SIZE
    int total_entry_num = (n/PLOG_PAGE_SIZE) + (n % PLOG_PAGE_SIZE ? 1 : 0);
    int lastsgl_entry_num = n % ENTRY_PER_SGL;
    int sgl_chain_cnt = (total_entry_num/ENTRY_PER_SGL) + (lastsgl_entry_num ? 1 : 0);
    assert(total_entry_num == (sgl_chain_cnt*ENTRY_PER_SGL+lastsgl_entry_num));
    std::vector<std::vector<void*>> data;

    init_plog_io_ctx(&read_ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    rw_info.offset = offset / PLOG_RW_MIN_GRLTY;
    rw_info.length = plog_calc_rw_len(n,PLOG_RW_MIN_GRLTY);
    rw_info.plog_param.plog_id = std::strtoll(plog_id.data(), nullptr,0);
    rw_info.plog_param.access_id = 0;
    rw_info.plog_param.pg_version = 0;
    rw_info.opt = PLOG_RW_OPT_READ;

    sgl_vector = static_cast<sgl_vector_t*>(calloc(
        1, sizeof(*sgl_vector) + (sgl_chain_cnt * sizeof(sgl_vector->sgls[0]))));
    if (sgl_vector == nullptr) {
      return Status::IOError(plog_id, "calloc sgl_vector failed");
    }

    for(int i=0; i<sgl_chain_cnt; i++){
      std::vector<void*> edata;
      for(int j=0; j<(i==sgl_chain_cnt-1)?lastsgl_entry_num:ENTRY_PER_SGL; j++){
        void *rdata = nullptr;
        rdata = rte_malloc(nullptr, PLOG_PAGE_SIZE + PLOG_DIF_AREA_SPACE, RTE_ALIGN);
        if(rdata == nullptr){
          return Status::IOError(plog_id, "rte_malloc failed");
        }
        edata.push_back(rdata);
      }
      data.push_back(edata);
    }

    sgl_vector->cnt = sgl_chain_cnt;
    for(int i=0; i<sgl_chain_cnt; i++){
      struct plog_sgl_s sgl = {0};
      sgl.nextSgl = nullptr;
      sgl.entrySumInChain = 0x01;
      sgl.entrySumInSgl = 0x01;
      for(int j=0; j<(i==sgl_chain_cnt-1)?lastsgl_entry_num:ENTRY_PER_SGL; j++){
        sgl.entrys[j].buf = (char*)data[i][j];
        sgl.entrys[j].len = PLOG_PAGE_SIZE + PLOG_DIF_AREA_SPACE;
      }
      sgl_vector->sgls[i] = &sgl;
    }

    plog_read_sgl(plog_disk_fd_, &rw_info, sgl_vector, &read_ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError(plog_id, "plog read failed");
    }
    if(iscopy){
      return Status::OK();
    }
    std::string sdata = "";
    for(int i=0; i<sgl_vector->cnt; i++){
      struct plog_sgl_s* sgl = sgl_vector->sgls[i];
      for(int j=0; j<sgl->entrySumInSgl;j++){
        sdata += sgl->entrys[j].buf;
      }
    }
    std::strcpy(scratch, sdata.data());
    *result = Slice(scratch,n);
    for(int i=0; i<sgl_chain_cnt; i++){
      for(int j=0; j<(i==sgl_chain_cnt-1)?lastsgl_entry_num:ENTRY_PER_SGL; j++){
        rte_free(data[i][j]);
      }
    }
    free(sgl_vector);
    return Status::OK();
  }

  static Status DoRemove(std::string plog_id){
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = std::strtoll(plog_id.data(), nullptr,0);
    plog_delete(PlogFile::plog_disk_fd_,&plog_param,&ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(PlogFile::plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError(plog_id,"RemoveFile Failed");
    }
    return Status::OK();
  }

  static Status DoSeal(std::string plog_id){
    struct plog_rw_status plog_status;
    struct plog_io_ctx ctx = {0};
    struct plog_param plog_param = {0};
    init_plog_io_ctx(&ctx, plog_rw_done, &plog_status);
    plog_status_init(&plog_status);
    plog_param.plog_id = std::strtoll(plog_id.data(), nullptr,0);
    uint64_t seal_size = GetSize(plog_id);
    if(seal_size < 0){
      return Status::IOError("Close " + plog_id, "seal_size = -1");
    }
    plog_seal(plog_disk_fd_,&plog_param,seal_size,PLOG_SEAL_REASON,&ctx);
    while (!is_plog_cmd_done(&plog_status)) {
      plog_process_completions(plog_disk_fd_);
      usleep(COMPLETION_USLEEP_TIME);
    }
    if (!is_plog_status_success(&plog_status)) {
      return Status::IOError("Close " + plog_id,"seal failed");
    }
    return Status::OK();
  }

  static Status CopyPlog(const std::string src, const std::string dst){
    //get size of src plog
    uint64_t src_len = GetSize(src);
    if(src_len == -1){
      return Status::IOError(src,"Size = -1");
    }
    sgl_vector_t *sgl_vector = nullptr;
    DoRead(src,0,src_len, nullptr, nullptr, sgl_vector, true);
    struct plog_rw_status plog_status;
    Status s = DoCreate(dst);
    if(!s.ok()){
      return Status::IOError(dst, "Create dst falied");
    }
    s = DoAppend(dst,sgl_vector,&plog_status,src_len);
    if(!s.ok()){
      return Status::IOError(dst, "Append dst falied");
    }
    s = DoSeal(dst);
    if(!s.ok()){
      return Status::IOError(dst, "Seal dst falied");
    }
    s = DoRemove(src);
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
      files_.erase(plog_id_);
      DoRemove(plog_id_);
      delete this;
    }
  }

  uint64_t Size(){
    return GetSize(plog_id_);
  }

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const{
    sgl_vector_t *sgl_vector = nullptr;
    return DoRead(plog_id_,offset,n,result,scratch,sgl_vector, false);
  }

  //must be called at first when NewWritableFile
  Status Create(){
    if(plog_disk_fd_ < 1){
      return Status::IOError(plog_id_,"plog_disk_fd_ < 1");
    }
    Status s = DoCreate(plog_id_);
    if(!s.ok()){
      flag_ = false;
      return Status::IOError(plog_id_,"plog_disk_fd_ < 1");
    }
    flag_ = true;
    return Status::OK();
  }

  Status Append(const Slice& data){
    if(plog_disk_fd_ < 1){
      return Status::IOError(plog_id_,"plog_disk_fd_ < 1");
    }
    if(!flag_ && !is_manifest_){
      return Status::IOError(plog_id_,"flag_ is false");
    }
    const char* src = data.data();
    size_t src_len = data.size();
    if(p_status_== nullptr){
      p_status_ = write_status_pool_->Allocator(plog_id_);
    }
    struct plog_sgl_s sgl = {0};
    sgl_vector_t *sgl_vector = nullptr;
    void *data_rte = nullptr;
    int sgl_cnt = 1;
    assert((src_len+PLOG_DIF_AREA_SPACE)<=PLOG_PAGE_SIZE);
    sgl_vector = static_cast<sgl_vector_t*>(calloc(
        1, sizeof(*sgl_vector) + (sgl_cnt * sizeof(sgl_vector->sgls[0]))));
    if (sgl_vector == nullptr) {
      return Status::IOError(plog_id_, "calloc sgl_vector failed");
    }
    data_rte = rte_malloc(nullptr, src_len + PLOG_DIF_AREA_SPACE, RTE_ALIGN);
    if(data== nullptr){
      return Status::IOError(plog_id_, "rte_malloc failed");
    }
    std::strcpy(static_cast<char*>(data_rte),src);
    sgl_vector->cnt = sgl_cnt;
    sgl.nextSgl = nullptr;
    sgl.entrySumInChain = 0x01;
    sgl.entrySumInSgl = 0x01;
    sgl.entrys[0].buf = (char*)data_rte;
    sgl.entrys[0].len = src_len + PLOG_DIF_AREA_SPACE;
    sgl_vector->sgls[0] = &sgl;
    DoAppend(plog_id_,sgl_vector,p_status_,src_len);
    rte_free(data_rte);
    free(sgl_vector);
    return Status::OK();
  }

  Status Remove(){
    MutexLock lock(&refs_mutex_);
    do_delete_ = true;
    if(refs_<=0){
      return DoRemove(plog_id_);
    }
    return Status::OK();
  }

  Status Close(){
    Status s;
    if(plog_disk_fd_ < 1){
      return Status::IOError("Close " + plog_id_,"plog_disk_fd_ < 1");
    }
    DoSeal(plog_id_);
    //deallocator p_status in the write_status_pool_
    if(p_status_!= nullptr){
      s = write_status_pool_->Deallocator(plog_id_,p_status_);
    }
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
//port::Mutex PlogFile::writefile_mutex_;
//std::map<std::string,PlogFile*> PlogFile::writefile_ GUARDED_BY(PlogFile::writefile_mutex_);

class PlogSequentialFile final : public SequentialFile{
 public:
  PlogSequentialFile(PlogFile* plog_file)
      : plog_file_(plog_file), pos_(0) { plog_file_->Ref();}
  ~PlogSequentialFile() override { plog_file_->Unref();}

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
      return Status::IOError(plog_file_->plog_id_, "pos_ > plog_file->Size()");
    }
    const uint64_t available = plog_file_->Size() - pos_;
    if(n>available){
      n = available;
    }
    pos_ += n;
    return Status::OK();
  }
 private:
  PlogFile *plog_file_;
  uint64_t pos_;
};

class PlogRandomAccessFile final : public RandomAccessFile{
 public:
  PlogRandomAccessFile(PlogFile* plog_file): plog_file_(plog_file) { plog_file_->Ref();}
  ~PlogRandomAccessFile() override { plog_file_->Unref();}
  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override{
    return plog_file_->Read(offset,n,result,scratch);
  }

 private:
  PlogFile *plog_file_;
};

class PlogWritableFile final :  public WritableFile{
 public:
  PlogWritableFile(PlogFile *plog_file)
      : plog_file_(plog_file), pos_(0) { plog_file_->Ref();}
  ~PlogWritableFile() override{
    if(plog_file_!= nullptr){
      Close();
    }
  }

  Status Append(const Slice& data) override{
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
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
    if (write_size < kWritableFileBufferSize) {
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
    //plog_file_ = NULL;
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
        return Status::IOError(plog_file_->plog_id_, "Sync MANIFEST: Other files haven't write to plog");
      }
    }else{
      return FlushBuffer();
    }
  }

 private:

  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  Status WriteUnbuffered(const char* data, size_t size) {
    Status s = plog_file_->Append(Slice(data,size));
    return s;
  }

  char buf_[kWritableFileBufferSize];
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
      return PlogFile::CopyPlog(from,to);
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