

### PLOG_LEVELDB

---

#### BACKGROUND



---

#### PLOG

| API                                                          |                                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| **1.** ` int plog_fabric_init(); `                           | initiate memory and module                                   |
| **2.** `int plog_create_ctrlr(struct plog_disk *pdisk, struct plog_io_ctx *ctx);` | create device(client should assign `plog_disk.traddr/trsvcid/esn/nsid, plog_io_ctx`) |
| **3.** ` int plog_open(struct plog_disk *pdisk);`            | open device,client should assign `plog_disk.traddr/trsvcid/esn/nsid, return fd` |
| `int plog_create(int fd, struct plog_param *pinfo, struct plog_attr_table *plog_attr, struct plog_io_ctx *ctx);` | client should assign `fd, plog_param.plog_id, plog_attr_table.create_size,plog_io_ctx` |
| `int plog_get_attr(int fd, struct plog_param *pinfo, struct plog_attr_table *plog_attr, struct plog_io_ctx *ctx);` | client should assign `fd, plog_param.plog_id, plog_io_ctx`, the results will be stored in `plog_attr` |
| `int plog_append_sgl(int fd, struct plog_rw_param *param, sgl_vector_t *sgl, struct plog_io_ctx *ctx);` | client should assign `fd, plog_rw_param.opt/plog_param/offset/length, sgl, plog_io_ctx ` |
| `int plog_read_sgl(int fd, struct plog_rw_param *param, sgl_vector_t *sgl, struct plog_io_ctx *ctx);` | `fd, plog_rw_param.opt, sgl, plog_io_ctx`                    |
| `int plog_seal(int fd, struct plog_param *pinfo, uint64_t seal_size, uint8_t seal_reason, struct plog_io_ctx *ctx);` | `fd, plog_param.plog_id, plog_io_ctx`; `seal_size=0, seal_reason=3` |
| `int plog_delete(int fd, struct plog_param *pinfo, struct plog_io_ctx *ctx);` |                                                              |
| **5.** `void plog_close(int fd);`                            |                                                              |
| **6.** `void plog_delete_ctrlr(struct plog_disk *pdisk);`    |                                                              |
| **7.** `void plog_fabric_fini();`                            |                                                              |
| `void *rte_malloc(const char *type, size_t size, unsigned align); ` | `type` can be `NULL`; `size` to be allocated; `align` if it's not 0, the return is a pointer that is multiple of `align` |
| `void rte_free(void *addr);`                                 | free                                                         |
| `free(sgls)`                                                 |                                                              |

##### 1.`sgl_vector`

<img src=".\fig\PLOG_layout.png" alt="PLOG_layout" style="zoom: 67%;" />

##### 2.`plog_append_sgl`

we can get`read/append minimum granularity(PLOG_RW_MIN_GRLTY) `when we connect to `plog disk`, but current `PLOG_RW_MIN_GRLTY` is a fixed number (16B).

1. call `plog_get_attr` to get the `plog_attr_table` of the `plog`, what we really need here is the `written_size` of the `plog`

2. set `plog_rw_param.offset` where to start write to the `plog` which is `attr.written_size/PLOG_RW_MIN_GRLTY`

3. set `plog_rw_param.length`, the length of data that is about to write into `plog`

   ```cpp
   plog_rw_param.length = plog_calc_rw_len(data_len, PLOG_RW_MIN_GRLTY);
   static inline uint32_t plog_calc_rw_len(uint32_t data_len, uint16_t gran){
       if (data_len == 0) {
           return 0;
       }
       return ((data_len + gran - 1) / gran - 1);  /* 0 base value, 向上取整再减一 */
   }
   ```

4. set `plog_id`& read/write operation type

   ```cpp
   rw_info.plog_param.plog_id = plog_id;
   rw_info.plog_param.access_id = 0;
   rw_info.plog_param.pg_version = 0;
   rw_info.opt = PLOG_RW_OPT_APPEND;
   ```

5. allocate `sgl_vector_t *sgls`

   ```cpp
   typedef struct sgl_vector {
       uint32_t cnt;              /* sgl链数量 */
       struct plog_sgl_s *sgls[]; /* sgls memory size is enqual to cnt*sizeof(struct plog_sgl*), which is malloced by user                           */
   } sgl_vector_t;
   sgl_vector_t *sgls = NULL;
   sgls = calloc(1, sizeof(*sgls) + (sgl_cnt * sizeof(sgls->sgls[0])));
   ```

6. allocate `data`

   ```cpp
   data = rte_malloc(NULL, data_len + dif_len, PLOG_PAGE_SIZE);
   ```

7. write to `data` , `sgls` points to `data`

   ```cpp
   sgls->cnt = sgl_cnt;
   sgl.nextSgl = NULL;
   sgl.entrySumInChain = 0x01;
   sgl.entrySumInSgl = 0x01;
   gen_write_data(data, data_len); //write to data
   sgl.entrys[0].buf = data;
   sgl.entrys[0].len = data_len + dif_len;
   sgls->sgls[0] = &sgl;
   ```

##### 3.`plog_read_sgl`

1. set `plog_rw_param.opt = PLOG_RW_OPT_READ`

2. allocate `sgl_vector_t *sgls`

   ```cpp
   sgl_vector_t *sgls = calloc(1, sizeof(*sgls) + (sgl_cnt * sizeof(sgls->sgls[0])));
   ```

3. allocate `data` as buf

   ```cpp
   data = rte_malloc(NULL, data_len + dif_len, PLOG_PAGE_SIZE);
   memset(data, 0x0, data_len + dif_len);
   ```

4.  see Q4

##### 4. Questions

1. How do we set `plog_io_ctx`? Just like what `plog_fabric.c` does?

   - ```cpp
     enum plog_done {
         PLOG_FABRIC_TESTING,
         PLOG_FABRIC_TEST_DONE,
         PLOG_FABRIC_TEST_BUTT,
     };
     enum plog_status {
         PLOG_FABRIC_TEST_STATUS_SUCCESS,
         PLOG_FABRIC_TEST_STATUS_TIMEOUT,
         PLOG_FABRIC_TEST_STATUS_FAILED,
         PLOG_FABRIC_TEST_STATUS_BUTT,
     };
     static struct status {
         enum plog_status status;
         enum plog_done done;
     } g_status;
     void test_done(int status_code, int code_type, void *cb_arg)
     {
         struct status *status = (struct status *)cb_arg;
         if (status_code == 0 && code_type == 0) {
             status->status = PLOG_FABRIC_TEST_STATUS_SUCCESS;
         } else {
             status->status = PLOG_FABRIC_TEST_STATUS_FAILED;
         }
     
         printf("done : status_code(0x%X) code_type(0x%x)\n", status_code, code_type);
         status->done = PLOG_FABRIC_TEST_DONE;
     }
     ctx->cb = test_done;
     ctx->cb_arg = &g_status;
     ctx->timeout_ms = 12*1000;
     ```

   - 

2. what is `  PLOG_DIF_AREA_SPACE = 64`? why should we allocate another 64 B memory? `plog_fabric.c` line 209/222

   - checksum, can be 0

3. what is the size of each entry in `sgl`? should it be `PLOG_PAGE_SIZE = 4096 `? `plog_fabric.c` line 222

   - it depends on the user

4. will `plog_read_sgl` read all the data from the `plog`? 

   - No, we can set the offset and length of the data

5. How to set the `seal_size` and `seal_reason`?

---

#### ZOOKEEPER

##### C API

[Zookeeper C API学习总结 - 云+社区 - 腾讯云 (tencent.com)](https://cloud.tencent.com/developer/article/1142948)

```CPP
ZOOAPI zhandle_t *zookeeper_init(const char *host, watcher_fn fn,
                                 int recv_timeout,
                                 const clientid_t * clientid,
                                 void *context, int flags);
//Synchronous
ZOOAPI int zoo_create(zhandle_t * zh, const char *path,
                      const char *value, int valuelen,
                      const struct ACL_vector *acl, int flags,
                      char *path_buffer, int path_buffer_len);

ZOOAPI int zoo_delete(zhandle_t * zh, const char *path, int version);

ZOOAPI int zoo_exists(zhandle_t * zh, const char *path, int watch,
                      struct Stat *stat);

ZOOAPI int zoo_get(zhandle_t * zh, const char *path, int watch,
                   char *buffer, int *buffer_len, struct Stat *stat);

ZOOAPI int zoo_get_children(zhandle_t * zh, const char *path,
                            int watch, struct String_vector *strings);
//Asynchronous
ZOOAPI int zoo_acreate(zhandle_t * zh, const char *path,
                       const char *value, int valuelen,
                       const struct ACL_vector *acl, int flags,
                       string_completion_t completion, const void *data);
ZOOAPI int zoo_awget(zhandle_t * zh, const char *path,
                     watcher_fn watcher, void *watcherCtx,
                     data_completion_t completion, const void *data);

ZOOAPI int zoo_aset(zhandle_t * zh, const char *path,
                    const char *buffer, int buflen, int version,
                    stat_completion_t completion, const void *data);

ZOOAPI int zoo_aget_children(zhandle_t * zh, const char *path,
                             int watch,
                             strings_completion_t completion,
                             const void *data);

ZOOAPI int zoo_awget_children(zhandle_t * zh, const char *path,
                              watcher_fn watcher, void *watcherCtx,
                              strings_completion_t completion,
                              const void *data);

ZOOAPI int zoo_aget_children2(zhandle_t * zh, const char *path,
                              int watch,
                              strings_stat_completion_t completion,
                              const void *data);

ZOOAPI int zoo_awget_children2(zhandle_t * zh, const char *path,
                               watcher_fn watcher, void *watcherCtx,
                               strings_stat_completion_t completion,
                               const void *data);

ZOOAPI int zoo_async(zhandle_t * zh, const char *path,
                     string_completion_t completion, const void *data);
```

##### `Znode` Type

| Type        | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| Persistence | Persistence `znode` is alive even after the client, which created that particular `znode`, is disconnected. By default, all `znodes` are persistent unless otherwise specified. |
| Ephemeral   | Ephemeral `znodes` are active until the client is alive. When a client gets disconnected from the `ZooKeeper` ensemble, then the ephemeral `znodes` **deleted automatically**. For this reason, only ephemeral `znodes` are not allowed to have a children further. If an ephemeral `znode` is deleted, then the next suitable node will fill its position. Ephemeral `znodes` play an important role in Leader election. |
| Sequential  | Sequential `znodes` can be either persistent or ephemeral. When a new `znode` is created as a sequential `znode`, then `ZooKeeper` sets the path of the `znode` by attaching a 10 digit sequence number to the original name. |

---

#### FILE TYPE

##### 1.`LevelDB` Files

Each time when we launch `LevelDB`, a new `MANIFEST` file will be created. Hence, there might be existing multiple `MANIFEST` files. We use `CURRENT` to record which one is the most recent `MANEFIST`.

| File Type         |                                                    | PLOG_LEVELDB                                            |
| ----------------- | -------------------------------------------------- | ------------------------------------------------------- |
| `kLogFile`        | `[0-9]+.log` ,WAL                                  |                                                         |
| `kDBLockFile`     | `LOCK`                                             | `zookeeper`                                             |
| `kTableFile`      | `[0-9]+.ldb ` ,SSTable                             |                                                         |
| `kDescriptorFile` | `MANEFIST-[0-9]+`                                  | add more information to it(e.g. `log`,stale `MANEFIST`) |
| `kCurrentFile`    | `CURRENT`                                          | `zookeeper`                                             |
| `kTempFile`       | `[0-9]+.dbtmp` , temporary file while repairing DB | not exists, we place `CURRENT` on `zookeeper`           |
| `kInfoLogFile`    | `LOG` , information log file while running DB      |                                                         |

##### 2. `MANEFIST`

```cpp
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9
};
```

<img src=".\fig\MANEFIST.png" alt="MANEFIST" style="zoom:67%;" />

---

#### ENV

| Class              | Description                                                  |
| ------------------ | ------------------------------------------------------------ |
| `Env`              |                                                              |
| `SequentialFile`   | A file abstraction for reading sequentially through a file   |
| `RandomAccessFile` | A file abstraction for randomly reading the contents of a file. |
| `WritableFile`     | A file abstraction for sequential writing.  The implementation must provide buffering since callers may append small fragments at a time to the file. |
| `Logger`           | An interface for writing log messages.                       |
| `FileLock`         | Identifies a locked file.                                    |
| `EnvWrapper`       | An implementation of `Env` that forwards all calls to another `Env`. May be useful to clients who wish to override just part of the functionality of another `Env`. |

##### 1. `Env`&`EnvWrapper`  

| `Env/EnvWrapper`                                             | Description                                                  |
| :----------------------------------------------------------- | ------------------------------------------------------------ |
| `Env* target_;`   (`EnvWrapper`)                             | member variable of `EnvWrapper`                              |
| `Env()`;`EnvWrapper(Env* t)`;`                              target_(t)`(`EnvWrapper`) |                                                              |
| `~Env()`;`~EnvWrapper()`                                     | `virtual`                                                    |
| `static Env* Default()`                                      | Return a default environment suitable for the current OS.    |
| `Status NewSequentialFile(const std::string& fname, SequentialFile** result)` | Create an object that sequentially reads the file with `fname`. On success, stores a pointer to the new file in `*result` and returns OK. On failure stores `nullptr` in `*result` and returns non-OK.  If the file does not exist, returns a non-OK status.  Implementations should return a  `NotFound` status when the file does not exist. **The returned file will only be accessed by one thread at a time.** |
| `Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)` | Create an object supporting random-access reads from the file with `fname`. **The returned file may be concurrently accessed by multiple threads.** |
| `Status NewWritableFile(const std::string& fname,  WritableFile** result) ` | Create an object that writes to a new file with `fname`. Deletes any existing file with the same name and creates a new file.  On success, stores a pointer to the new file in `*result` and returns OK.  On failure stores `nullptr` in `*result` and returns non-OK. **The returned file will only be accessed by one thread at a time.** |
| `Status NewAppendableFile(const std::string& fname, WritableFile** result)` | Create an object that either appends to an existing file, or writes to a new file (if the file does not exist to begin with). **The returned file will only be accessed by one thread at a time.** |
| `bool FileExists(const std::string& fname)`                  | Returns true iff the named file exists.                      |
| `Status GetChildren(const std::string& dir, std::vector<std::string>* result)` | Store in `*result` the names of the children of `dir`. Original contents of `*results` are dropped. |
| `Status RemoveFile\DeleteFile(const std::string& fname)`     | Delete the named file. The default implementation calls `DeleteFile` . Updated `Env` implementations must override `RemoveFile` and ignore the existence of `DeleteFile`. |
| `Status CreateDir(const std::string& dirname)`               | Create the specified directory.                              |
| `Status RemoveDir/DeleteDir(const std::string& dirname)`     | Delete the specified directory. must call `RemoveDir` instead of `DeleteDir`. |
| `Status GetFileSize(const std::string& fname, uint64_t* file_size)` | Store the size of `fname` in `*file_size`.                   |
| `Status RenameFile(const std::string& src, const std::string& target)` | Rename file `src` to `target`.                               |
| `Status LockFile(const std::string& fname, FileLock** lock)` | Lock the specified file. Used to prevent concurrent access to the same `db` by multiple processes.  On failure, stores `nullptr` in `*lock` and returns non-OK. On success, stores a pointer to the object that represents the acquired lock in `*lock `and returns OK.  The caller should call `UnlockFile(*lock)` to release the `lock`.  If the process exits, the lock will be automatically released. If somebody else already holds the lock, finishes immediately with a failure. |
| `Status UnlockFile(FileLock* lock)`                          | Release the lock acquired by a previous successful call to `LockFile`. |
| `void Schedule(void (*function)(void* arg), void* arg)`      | Arrange to run "`(*function)(arg)`" once in a background thread. |
| `void StartThread(void (*function)(void* arg), void* arg)`   | Start a new thread, invoking "`function(arg)`" within the new thread. |
| `Status GetTestDirectory(std::string* path)`                 | `*path` is set to a temporary directory that can be used for testing. |
| `Status NewLogger(const std::string& fname, Logger** result)` | Create and return a log file for storing informational messages. |
| `uint64_t NowMicros()`                                       | Returns the number of micro-seconds since some fixed point in time. Only useful for **computing deltas of time.** |
| `void SleepForMicroseconds(int micros)`                      | Sleep/delay the thread for the prescribed number of micro-seconds. |

##### 2. `SequentialFile`

Only used to read log information(see `log_reader.cc`)

| Methods                                               |                                                              |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| `Status Read(size_t n, Slice* result, char* scratch)` | Read up to "`n`" bytes from the file.  "`scratch[0..n-1]`" may be written by this routine.  Sets "`*result`" to the data that was  read (including if fewer than "n" bytes were successfully read).  May set "`*result`" to point at data in "`scratch[0..n-1]`", so "`scratch[0..n-1]`" must be live when "`*result`" is used. If an error was encountered, returns a non-OK status. |
| `Status Skip(uint64_t n)`                             | Skip "n" bytes from the file.                                |

##### 3. `RandomAccessFile`

| Method                                                       |                                                          |
| ------------------------------------------------------------ | -------------------------------------------------------- |
| `Status Read(uint64_t offset, size_t n, Slice* result,char* scratch)` | Read up to `n` bytes from the file starting at `offset`. |

##### 4. `WritableFile`  

| Methods                            |      |
| ---------------------------------- | ---- |
| `Status Append(const Slice& data)` |      |
| `Status Close()`                   |      |
| `Status Flush()`                   |      |
| `Status Sync()`                    |      |

##### 5. `Logger`  

| Methods                                          |                                                           |
| ------------------------------------------------ | --------------------------------------------------------- |
| `void Logv(const char* format, std::va_list ap)` | Write an entry to the log file with the specified format. |

##### 6. `FileLock`  



---

#### MEMENV

| Class/Method                    | Description                          |
| ------------------------------- | ------------------------------------ |
| `FileState`                     | information of a file(blocks & size) |
| `SequentialFileImpl`            | inherit `SequentialFile`             |
| `RandomAccessFileImpl`          | inherit `RandomAccessFile`           |
| `WritableFileImpl`              | inherit `WritableFile`               |
| `NoOpLogger`                    | inherit `Logger`                     |
| `InMemoryEnv`                   | inherit `EnvWrapper`                 |
| `Env* NewMemEnv(Env* base_env)` | `return new InMemoryEnv(base_env)`   |

##### 1. `FileState`

1. Member Variable

   | Variable                     | Description                                               |
   | ---------------------------- | --------------------------------------------------------- |
   | `kBlockSize`                 | Block size is $8*1024$                                    |
   | `port::Mutex refs_mutex`     |                                                           |
   | `int refs_`                  | `GUARDED_BY(refs_mutex)`, the reference count of the file |
   | `port::Mutex blocks_mutex`   |                                                           |
   | `std::vector<char*> blocks_` | `GUARDED_BY(blocks_mutex)`, contents of the file          |
   | `uint64_t size_`             | `GUARDED_BY(blocks_mutex)`, size of the file              |

   `ref_` records the # of 

2. Member Methods

   | Method                                                       | Description                                                  |
   | ------------------------------------------------------------ | ------------------------------------------------------------ |
   | `FileState()`                                                | `refs_(0), size_(0)`, no copying allowed                     |
   | `void Ref()`                                                 | increase the reference count                                 |
   | `void Unref()`                                               | decrease the reference count, delete if it is the last reference |
   | `uint64_t Size()`                                            | return size of the file                                      |
   | `void Truncate()`                                            | delete file (delete blocks and set size to 0)                |
   | `Status Read(uint64_t offset, size_t n, Slice* result, char* scratch)` | read up to $n$ bytes from the file starting at `offset`      |
   | `Status Append(const Slice& data)`                           | append `data` to the end of the file                         |

3. 

##### 2. `SequentialFileImpl`

1. Member Variable

   | Variable           | Description                          |
   | ------------------ | ------------------------------------ |
   | `FileState* file_` |                                      |
   | `uint64_t pos_`    | current reading position of the file |

2. Member Method

   | Method                                                | Description                                                  |
   | ----------------------------------------------------- | ------------------------------------------------------------ |
   | `SequentialFileImpl(FileState* file)`                 | `file_(file), pos_(0),file_->Ref()`                          |
   | `~SequentialFileImpl()`                               | `file_->Unref()`                                             |
   | `Status Read(size_t n, Slice* result, char* scratch)` | read up to $n$ bytes from the file starting at `pos_`, update `pos_` |
   | `Status Skip(uint64_t n)`                             | update `pos_` by adding $n$                                  |

3. 

##### 3. `RandomAccessFileImpl`

1. Member Variable

   | Variable           | Description |
   | ------------------ | ----------- |
   | `FileState* file_` |             |

2. Member Method

   | Method                                                       | Description                                             |
   | ------------------------------------------------------------ | ------------------------------------------------------- |
   | `RadomAccessFileImpl(FileState* file)`                       | `file_(file),file_->Ref()`                              |
   | `~RadomAccessFileImpl()`                                     | `file_->Unref()`                                        |
   | `Status Read(uint64_t offset, size_t n, Slice* result, char* scratch)` | read up to $n$ bytes from the file starting at `offset` |

3. 

##### 4. `WritableFileImpl`

1. Member Variable

   | Variable           | Description |
   | ------------------ | ----------- |
   | `FileState* file_` |             |

2. Member Method

   | Variable                            | Description                |
   | ----------------------------------- | -------------------------- |
   | `WritableFileImpl(FileState* file)` | `file_(file),file_->Ref()` |
   | `~WritableFileImpl()`               | `file_->Unref()`           |
   | `Status Append(const Slice& data)`  | `file_->Append(data)`      |
   | `Status Close()`                    | do nothing                 |
   | `Status Flush()`                    | do nothing                 |
   | `Status Sync()`                     | do nothing                 |

3. 

##### 5. `NoOpLogger`

do nothing

##### 6. `InMemoryEnv`

1. Member Variable

   | Variable                                               | Description                               |
   | ------------------------------------------------------ | ----------------------------------------- |
   | `typedef std::map<std::string, FileState*> FileSystem` | map from filenames to `FileState` objects |
   | `port::Mutex mutex_`                                   |                                           |
   | `Filesystem file_map_`                                 | `GUARDED_BY(mutex_)`                      |
   | `Env* target`_                                         | inherit from `EnvWrapper`                 |

2. Member Method

   | Methods                                                      | Description                                                  |
   | ------------------------------------------------------------ | ------------------------------------------------------------ |
   | `InMemoryEnv(Env* base_env)`                                 | `EnvWrapper(base_env):target_=base_env`                      |
   | `~InMemoryEnv()`                                             | `Unref` all the files in `file_map_`                         |
   | `Status NewSequentialFile(const std::string& fname,SequentialFile** result) ` | find file with `fname` in `file_map_`;` new SequentialFileImpl` |
   | `Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)` | find file with `fname` in `file_map_`;`new RandomAccessFileImpl` |
   | `Status NewWritableFile(const std::string& fname, WritableFile** result)` | if file with `fname` doesn't exist, create one; if exits, `truncate()`; `new WritableFileImpl` |
   | `Status NewAppendableFile(const std::string& fname, WritableFile** result)` | if file with `fname` doesn't exist, create one; `new WritableFileImpl` |
   | `bool FileExists(const std::string& fname)`                  | if file exists                                               |
   | `Status GetChildren(const std::string& dir, std::vector<std::string>* result)` | get all files under `dir`                                    |
   | `void RemoveFileInternal(const std::string& fname)`          | `EXCLUSIVE_LOCKS_REQUIRED(mutex_)`; `Unref & erase`file      |
   | `Status RemoveFile(const std::string& fname)`                | call `RemoveFileInternal`;                                   |
   | `Status CreateDir/RemoveDir(const std::string& dirname)`     | do nothing                                                   |
   | `Status GetFileSize(const std::string& fname, uint64_t* file_size)` | get file size                                                |
   | `Status RenameFile(const std::string& src, const std::string& target)` | rename file from `src` to `target`  by `RemoveFileInternal(target)` and assign `src` to `target` and then `erase(src)` |
   | `Status LockFile(const std::string& fname, FileLock** lock)` | do nothing                                                   |
   | ` Status UnlockFile(FileLock* lock)`                         | do nothing                                                   |
   | ` Status GetTestDirectory(std::string* path)`                | `*path = "/test"`                                            |
   | `Status NewLogger(const std::string& fname, Logger** result)` | do nothing                                                   |

3. 

---

#### ENV_POSIX



##### 1.`Limiter`

| Member                                |                                    |
| ------------------------------------- | ---------------------------------- |
| `std::atomic<int> acquires_allowed_;` | The number of available resources. |
| `bool Acquire()`                      |                                    |
| `void Release()`                      |                                    |



##### 2.`PosixSequentialFile`

| Member                                                |      |
| ----------------------------------------------------- | ---- |
| `const int fd_`                                       |      |
| `const std::string filename_`                         |      |
| `Status Read(size_t n, Slice* result, char* scratch)` |      |
| `Status Skip(uint64_t n)`                             |      |



##### 3.`PosixRandomAccessFile`

| Member Variable                |                                             |
| ------------------------------ | ------------------------------------------- |
| `const bool has_permanent_fd_` | If false, the file is opened on every read. |
| `const int fd_`                | -1 if has_permanent_fd_ is false.           |
| `Limiter* const fd_limiter_`   |                                             |
| `const std::string filename_`  |                                             |

| Member Method                                                |                                      |
| ------------------------------------------------------------ | ------------------------------------ |
| ` PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)` | `fd_limiter->Acquire()`              |
| `~PosixRandomAccessFile()`                                   | `close(fd_); fd_limiter_->Release()` |
| `Status Read(uint64_t offset, size_t n, Slice* result, char* `) |                                      |



##### 4.`PosixMmapReadableFile`

| Member Variable                 |      |
| ------------------------------- | ---- |
| `char* const mmap_base_;`       |      |
| `const size_t length_;`         |      |
| `Limiter* const mmap_limiter_;` |      |
| `const std::string filename_;`  |      |

| Member Method                                                |                                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| ` PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length,Limiter* mmap_limiter)` |                                                              |
| `~PosixMmapReadableFile()`                                   | `munmap(static_cast<void*>(mmap_base_);mmap_limiter_->Release();` |
| `Status Read(uint64_t offset, size_t n, Slice* result, char* `) |                                                              |

##### 5.`PosixWriteableFile`

| Member Variable                       |                                                           |
| ------------------------------------- | --------------------------------------------------------- |
| `char buf_[kWritableFileBufferSize];` | `buf_[0, pos_ - 1]` contains data to be written to `fd_`. |
| `size_t pos_;`                        |                                                           |
| `int fd_;`                            |                                                           |
| `const bool is_manifest_;`            | True if the file's name starts with MANIFEST.             |
| `const std::string filename_;`        |                                                           |
| `const std::string dirname_;`         | The directory of filename_.                               |

| Member Methods                                             |                                                              |
| ---------------------------------------------------------- | ------------------------------------------------------------ |
| `Status Append(const Slice& data)`                         |                                                              |
| `Status Close() `                                          |                                                              |
| `Status Flush() `                                          | `return FlushBuffer();`                                      |
| `Status Sync() `                                           | Ensure new files referred to by the manifest are in the filesystem. **This needs to happen before the manifest file is flushed to disk**, to avoid crashing in a state where the manifest refers to files that are not yet on disk. |
| `Status WriteUnbuffered(const char* data, size_t size)`    | write up to `size` bytes of `data` into `fd_`                |
| `Status FlushBuffer()`                                     | `WriteUnbuffered(buf_, pos_);`write `buf_[0, pos_ - 1]`  to  `fd_`. |
| `static Status SyncFd(int fd, const std::string& fd_path)` | Ensures that all the caches associated with the given file descriptor's data are flushed all the way to durable media, and can withstand power failures. |
| `Status SyncDirIfManifest()`                               | flush `MANEFIST` to storage                                  |
| `static std::string Dirname(const std::string& filename)`  | Returns the directory name in a path pointing to a file.     |
| ` static Slice Basename(const std::string& filename)`      | Extracts the file name from a path pointing to a file.       |
| `static bool IsManifest(const std::string& filename`)      | True if the given file is a manifest file.                   |



##### 6.`PosixFileLock`

```cpp
int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info;
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
  file_lock_info.l_whence = SEEK_SET;
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;  // Lock/unlock entire file.
  return ::fcntl(fd, F_SETLK, &file_lock_info);
}
```

| Members                         |                     |
| ------------------------------- | ------------------- |
| `const int fd_`                 |                     |
| `const std::string filename_`   |                     |
| `int fd()`                      | `return fd_;`       |
| `const std::string& filename()` | `return filename_;` |



##### 7.`PosixLockTable`

Tracks the files locked by `PosixEnv::LockFile()`. 

| Members                                                     |                    |
| ----------------------------------------------------------- | ------------------ |
| `port::Mutex mu_;`                                          |                    |
| `std::set<std::string> locked_files_`                       | `GUARDED_BY(mu_);` |
| `bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_)` |                    |
| `void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_)` |                    |



##### 8.`PosixEnv`

| Member                   |      |
| ------------------------ | ---- |
| background_work related  |      |
| `PosixLockTable locks_;` |      |
| `Limiter mmap_limiter_;` |      |
| `Limiter fd_limiter_; `  |      |

| `Env/EnvWrapper`                                             | Description                                                  |
| :----------------------------------------------------------- | ------------------------------------------------------------ |
| `static Env* Default()`                                      | Return a default environment suitable for the current OS.    |
| `Status NewSequentialFile(const std::string& fname, SequentialFile** result)` | `::open(); new PosixSequentialFile;` **The returned file will only be accessed by one thread at a time.** |
| `Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)` | if `mmap_limiter_.Acquire(),new PosixMmapReadableFile`;else `new PosixRandomAccessFile`  **The returned file may be concurrently accessed by multiple threads.** |
| `Status NewWritableFile(const std::string& fname,  WritableFile** result) ` | `::open(); new PosixWritableFile;`**The returned file will only be accessed by one thread at a time.** |
| `Status NewAppendableFile(const std::string& fname, WritableFile** result)` | `::open(); new PosixWritableFile;` **The returned file will only be accessed by one thread at a time.** |
| `bool FileExists(const std::string& fname)`                  | `::access(filename.c_str(), F_OK)`Returns true iff the named file exists. |
| `Status GetChildren(const std::string& dir, std::vector<std::string>* result)` | Store in `*result` the names of the children of `dir`. Original contents of `*results` are dropped. |
| `Status RemoveFile(const std::string& fname)`                | `::unlink(filename.c_str())`                                 |
| `Status CreateDir(const std::string& dirname)`               | `::mkdir(dirname.c_str(), 0755)`                             |
| `Status RemoveDir(const std::string& dirname)`               | `::rmdir(dirname.c_str())`                                   |
| `Status GetFileSize(const std::string& fname, uint64_t* file_size)` | Store the size of `fname` in `*file_size`.                   |
| `Status RenameFile(const std::string& src, const std::string& target)` | `std::rename(from.c_str(), to.c_str())`                      |
| `Status LockFile(const std::string& fname, FileLock** lock)` | `::open;locks_.Insert(filename);LockOrUnlock(fd, true);new PosixFileLock` |
| `Status UnlockFile(FileLock* lock)`                          | `LockOrUnlock(fd, false);locks_.Remove();::close`            |
| `void Schedule(void (*function)(void* arg), void* arg)`      | Arrange to run "`(*function)(arg)`" once in a background thread. |
| `void StartThread(void (*function)(void* arg), void* arg)`   | Start a new thread, invoking "`function(arg)`" within the new thread. |
| `Status GetTestDirectory(std::string* path)`                 | `*path` is set to a temporary directory that can be used for testing. |
| `Status NewLogger(const std::string& fname, Logger** result)` | Create and return a log file for storing informational messages. |
| `uint64_t NowMicros()`                                       | Returns the number of micro-seconds since some fixed point in time. Only useful for **computing deltas of time.** |
| `void SleepForMicroseconds(int micros)`                      | Sleep/delay the thread for the prescribed number of micro-seconds. |

##### 9.`SingletonEnv`

##### 10.`PosixLogger`



---

#### ENV_PLOG

##### High-level design principles

1. Place `CURRENT` file on `Zookeeper` (or `FS`); no `dbtmp`file
2. Place `LOCK` file on `Zookeeper` (or `FS`)
3. Add `log`(WAL) information to `MANEFIST`
4. 
5. 

##### `PlogCtxPool` 

Yes

##### `PlogSGL` 

Yes

`static PlogCtxPool *write_ctx_pool`

##### `PlogSequentialFile`

| Member                                                |                                                              |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| `const std::string plog_id_`                          |                                                              |
| `g_status g_status_`// stack                          |                                                              |
| `plog_io_ctx_t *ctx_` // stack                        |                                                              |
| `uint64_t pos_`                                       | current position of the `plog`                               |
| `bool is_current_`                                    | True if the file's name is `CURRENT`, we will go to `zookeeper`. We will call `ReadFiletoString` to read `CURRENT` |
| `PlogSequentialFile(std::string plog_id)`             | `plog_id_(plog_id)`,                                         |
| `~PlogSequentialFile()`                               | `free(ctx, g_status)`                                        |
| `Status Read(size_t n, Slice* result, char* scratch)` | call `plog_read_sgl`, `plog_process_completions` to see if read is success |
| `Status Skip(uint64_t n)`                             |                                                              |

##### `PlogRandomAccessFile`

`memenv`: `RandomAccessFile` vs `SequentialFile` is pretty much the same

`env_posix`: `RandomAccessFile` uses `pread` while `SequentialFile` uses `read`

| Member                                                       |                                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| `const std::string plog_id_`                                 |                                                              |
| `g_status g_status_` \\\stack                                |                                                              |
| `plog_io_ctx_t *ctx_ `\\\stack                               |                                                              |
| `PlogRadomAccessFileImpl(std::string plog_id)`               |                                                              |
| `~PlogRadomAccessFileImpl()`                                 |                                                              |
| `Status Read(uint64_t offset, size_t n, Slice* result, char* scratch)` | read up to $n$ bytes from the file starting at `offset`;call `plog_read_sgl` |

##### `PlogWritableFile`

| Member Variable                       |                                                              |
| ------------------------------------- | ------------------------------------------------------------ |
| `char buf_[kWritableFileBufferSize];` | `buf_[0, pos_ - 1]` contains data to be written to `plog`.   |
| `size_t pos_;`                        |                                                              |
| `const std::string plog_id_`          |                                                              |
| `g_status g_status_` //stack          |                                                              |
| `plog_io_ctx_t *ctx_` //stack         |                                                              |
| `const bool is_manifest_;`            | True if the file's name starts with `MANIFEST`.              |
| `const bool is_current_;`             | True if the file's name is `CURRENT`, we will go to `zookeeper` |

| Member Methods                                          |                                                              |
| ------------------------------------------------------- | ------------------------------------------------------------ |
| `Status Append(const Slice& data)`                      | the same as `PosixWritableFile::Append()`; need to re-write `WriteUnbuffered` |
| `Status Close() `                                       |                                                              |
| `Status Flush() `                                       | `FlushBuffer()`                                              |
| `Status Sync() `                                        | (Ensure new files referred to by the manifest are in the filesystem. **This needs to happen before the manifest file is flushed to disk**, to avoid crashing in a state where the manifest refers to files that are not yet on disk.) |
| `Status FlushBuffer()`                                  |                                                              |
| `Status WriteUnbuffered(const char* data, size_t size)` |                                                              |
| `static bool IsCurrent(const std::string& filename`)    | True if the given file is a `CURRENT` file                   |

##### `PlogLogger`

| Member Variable                                         |      |
| ------------------------------------------------------- | ---- |
| `const std::string plog_id_`                            |      |
| `g_status g_status_`                                    |      |
| `plog_io_ctx_t *ctx_`                                   |      |
| `void Logv(const char* format, std::va_list arguments)` |      |

##### `PlogFileLock`

| Members                         |                                                              |
| ------------------------------- | ------------------------------------------------------------ |
| `const std::string filename_`   | `znode`                                                      |
| `const std::string& filename()` | `return filename_;`                                          |
| `Insert()`                      | create `znode` with `filename_`, return true if it hasn't been created yet, otherwise, return false. When `leveldb` disconnect with `zookeeper`, it will be deleted so we don't need to delete it . |

##### `PlogEnv`

| Variable       |                            |
| -------------- | -------------------------- |
| read_ctx_pool  | lock/allocator/deallocator |
| write_ctx_pool | lock/allocator/deallocator |



1. 

| `Env`                                                        | Description                                                  |
| :----------------------------------------------------------- | ------------------------------------------------------------ |
| `static Env* Default()`                                      | Return a default environment suitable for the current OS.    |
| `Status NewSequentialFile(const std::string& fname, SequentialFile** result)` | **The returned file will only be accessed by one thread at a time.** |
| `Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result)` | **The returned file may be concurrently accessed by multiple threads.** |
| `Status NewWritableFile(const std::string& fname,  WritableFile** result) ` | **The returned file will only be accessed by one thread at a time.** |
| `Status NewAppendableFile(const std::string& fname, WritableFile** result)` | Same as `NewAppendableFile`;**The returned file will only be accessed by one thread at a time.** |
| `bool FileExists(const std::string& fname)`                  | only called to find `CURRENT` file when doing `Recover`      |
| `Status GetChildren(const std::string& dir, std::vector<std::string>* result)` | Read `MANEFIST` file to get `.log` `.ldb`                    |
| `Status RemoveFile\DeleteFile(const std::string& fname)`     | `UnRef`&`Delete`                                             |
| `Status CreateDir(const std::string& dirname)`               | NONE                                                         |
| `Status RemoveDir/DeleteDir(const std::string& dirname)`     | NONE                                                         |
| `Status GetFileSize(const std::string& fname, uint64_t* file_size)` | Store the size of `fname` in `*file_size`.**(where be called to decide which size to be used)** |
| `Status RenameFile(const std::string& src, const std::string& target)` | 1. if it's `CURRENT`, we operate on `zookeeper`; 2. if it tries to rename `RepairTable()`, write to a new `plog` with a larger suffix, and then delete the old one; 3. if it tries to rename `MANEFIST`, write to a new `plog` with `000001`(reset to 1);**TODO????** 4. if it tries to denote that the files are old (`ArchiveFile` or rename `.log`to`log.old` ), write to `MANEFIST`. |
| `Status LockFile(const std::string& fname, FileLock** lock)` | Used for `LOCK` file, `insert()` connect to `zookeeper`      |
| `Status UnlockFile(FileLock* lock)`                          | NONE                                                         |
| `void Schedule(void (*function)(void* arg), void* arg)`      | Arrange to run "`(*function)(arg)`" once in a background thread. |
| `void StartThread(void (*function)(void* arg), void* arg)`   | Start a new thread, invoking "`function(arg)`" within the new thread. |
| `Status GetTestDirectory(std::string* path)`                 | `*path` is set to a temporary directory that can be used for testing. |
| `Status NewLogger(const std::string& fname, Logger** result)` | Create and return a log file for storing informational messages. |
| `uint64_t NowMicros()`                                       | Returns the number of micro-seconds since some fixed point in time. Only useful for **computing deltas of time.** |
| `void SleepForMicroseconds(int micros)`                      | Sleep/delay the thread for the prescribed number of micro-seconds. |

##### Details 

1. `MANEFIST`

   ```CPP
   enum Tag {
     kComparator = 1,
     kLogNumber = 2,
     kNextFileNumber = 3,
     kLastSequence = 4,
     kCompactPointer = 5,
     kDeletedFile = 6,
     kNewFile = 7,
     // 8 was used for large value refs
     kPrevLogNumber = 9,
     // Add by PLOG
     kLogFile = 10,
     kManefistFile = 11
   };
   ```

   <img src=".\fig\MANEFIST_PLOG.png" alt="MANEFIST_PLOG" style="zoom:67%;" />

   

2. `RenameFile()`

   <img src=".\fig\RenameFile.png" alt="RenameFile" style="zoom:67%;" />

3. `GetChildren()`

   <img src=".\fig\GetChiledren.png" alt="GetChiledren" style="zoom: 67%;" />

4. Even though `leveldb` disconnect from `zookeeper`, it can still serve `write/read` request instead of turning to `read-only` mode. Since the only reason we connect zookeeper is to get `CURRENT` file and `LOCK` file. We will set the new `MANEFIST` filename in `CURRENT` when we open `DB`.


----

1. open channel
2. zns
3. kvs
4. 
