//
// Created by hgong on 7/2021.
//

#include <iostream>
#include <map>
#include <pthread.h>

#include "leveldb/env.h"
#include "leveldb/status.h"

#include "port/port.h"
#include "util/mutexlock.h"

#include "gtest/gtest.h"
#include "plog/include/plog_fabric.h"
#include "testutil.h"

namespace leveldb{

#define FILE0 64
#define FILE1 128
#define FILE2 256
#define FILE3 512
#define FILE4 1024
#define FILE5 2048

struct para{
  int num;
  leveldb::WritableFile* file;
};

void display_data(const unsigned char *buf, int len, int width, int group)
{
  int i;
  int offset = 0;
  int line_done = 0;
  char ascii[32 + 1];

  if (buf == NULL) {
    fprintf(stdout, "Error, data buf is null.");
    return;
  }

  fprintf(stdout, "     ");
  for (i = 0; i <= 0x0F; i++)
    fprintf(stdout, "%3X", i);
  for (i = 0; i < len; i++) {
    line_done = 0;
    if (i % width == 0)
      fprintf(stdout, "\n%04X:", offset);
    if (i % group == 0)
      fprintf(stdout, " %02X", buf[i]);
    else
      fprintf(stdout, "%02X", buf[i]);
    ascii[i % width] = (buf[i] >= '!' && buf[i] <= '~') ? buf[i] : '.';
    if (((i + 1) % width) == 0) {
      ascii[i % width + 1] = '\0';
      fprintf(stdout, " \"%.*s\"", width, ascii);
      offset += width;
      line_done = 1;
    }
  }
  if (!line_done) {
    unsigned b = width - (i % width);
    ascii[i % width + 1] = '\0';
    fprintf(stdout, " %*s \"%.*s\"", 0x02 * b + b / group + (b % group ? 1 : 0), "", width, ascii);
  }
  fprintf(stdout, "\n");
}
#define DISPLAY_DATA(_mem, _len) display_data(_mem, _len, 0x10, 1)

class EnvPlogTest : public testing::Test {
 public:
  EnvPlogTest() : env_(Env::Default()) {}
  Env* env_;
  static void write_file(void* arg){
    struct para* p;
    p = (struct para*)arg;
    int num = p->num;
    leveldb::WritableFile *file = p->file;
    for(int i=0; i<10; i++){
      std::string wdata;
      wdata = std::to_string(i)+"W0123456789";
      ASSERT_LEVELDB_OK(file->Append(Slice(wdata.data(),wdata.size())));
    }
    ASSERT_LEVELDB_OK(file->Sync());
    ASSERT_LEVELDB_OK(file->Close());
    std::cout << num << " File Closed!" << std::endl;
  }

  static void write_manifest(leveldb::WritableFile* file){
    std::string wdata = "M12334555";
    ASSERT_LEVELDB_OK(file->Append(Slice(wdata.data(),wdata.size())));
    ASSERT_LEVELDB_OK(file->Sync());
    ASSERT_LEVELDB_OK(file->Close());
    std::cout << "MANIFEST Closed!" << std::endl;
  }
};

TEST_F(EnvPlogTest, TestOnWritableFile){
  //test write
  std::string filename = std::to_string(FILE0);
  std::string wdata;
  wdata = "W0123456789_0123456789";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_, wdata.data(), filename));
  leveldb::WritableFile* file = nullptr;
  ASSERT_LEVELDB_OK(env_->NewWritableFile(filename, &file));
  void* data = rte_malloc(NULL, 4096+64, 16);
  memset((char*)data,0xAA,1024);
  memset((char*)data+1024,0xBB,1024);
  memset((char*)data+1024*2,0xCC,1024);
  memset((char*)data+1024*3,0xDD,1024);
  Slice sdata((char*)data,4096+64);
  ASSERT_LEVELDB_OK(file->Append(sdata));
  ASSERT_LEVELDB_OK(file->Flush());
  ASSERT_LEVELDB_OK(file->Close());
  ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
  rte_free(data);
}

TEST_F(EnvPlogTest, TestOnMultipleFiles){
  pthread_t tid[6];
  for(int i=0; i<6; i++){
    std::string pfilename = std::to_string(i) + std::to_string(FILE1);
    struct para p;
    p.num = i;
    ASSERT_LEVELDB_OK(env_->NewWritableFile(pfilename,&p.file));
    pthread_create(&tid[i],NULL,
                   reinterpret_cast<void* (*)(void*)>(EnvPlogTest::write_file),&p);
  }

  for(int i=0; i<6; i++) {
    pthread_join(tid[i], NULL);
    ASSERT_LEVELDB_OK(env_->RemoveFile(std::to_string(i) + std::to_string(FILE1)));
  }
}

TEST_F(EnvPlogTest,TestOnSequentialFile){
  std::string filename = std::to_string(FILE2);
  std::string wdata = "S01234567890_0123456789";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_,wdata.data(),filename));
  leveldb::WritableFile* wfile = nullptr;
  ASSERT_LEVELDB_OK(env_->NewWritableFile(filename, &wfile));
  void* data = rte_malloc(NULL, 4096+64, 16);
  memset((char*)data,0xAA,1024);
  memset((char*)data+1024,0xBB,1024);
  memset((char*)data+1024*2,0xCC,1024);
  memset((char*)data+1024*3,0xDD,1024);
  Slice sdata((char*)data,4096+64);
  ASSERT_LEVELDB_OK(wfile->Append(sdata));
  ASSERT_LEVELDB_OK(wfile->Flush());
  ASSERT_LEVELDB_OK(wfile->Close());
  leveldb::SequentialFile* sfile = nullptr;
  ASSERT_LEVELDB_OK(env_->NewSequentialFile(filename,&sfile));
  leveldb::Slice res;
  char* scratch = nullptr;
  ASSERT_LEVELDB_OK(sfile->Read(wdata.size(),&res,scratch));
  std::cout << "TestOnSequentialFile: " << res.data() << std::endl;
  ASSERT_LEVELDB_OK(sfile->Read(4096,&res,scratch));
 // DISPLAY_DATA(reinterpret_cast<const unsigned char*>(res.data()),4096);
  ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
}

TEST_F(EnvPlogTest, TestOnRandomAccessFile){
  std::string filename = std::to_string(FILE3);
  std::string wdata = "R01234567890_0123456789";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_,wdata.data(),filename));
  leveldb::WritableFile* file = nullptr;
  ASSERT_LEVELDB_OK(env_->NewWritableFile(filename, &file));
  void* data = rte_malloc(NULL, 4096, 16);
  memset((char*)data,0xAA,1024);
  memset((char*)data+1024,0xBB,1024);
  memset((char*)data+1024*2,0xCC,1024);
  memset((char*)data+1024*3,0xDD,1024);
  Slice sdata((char*)data,4096);
  ASSERT_LEVELDB_OK(file->Append(sdata));
  ASSERT_LEVELDB_OK(file->Flush());
  ASSERT_LEVELDB_OK(file->Close());
  leveldb::RandomAccessFile* files[5]={nullptr};
  for(int i=0; i<5; i++){
    ASSERT_LEVELDB_OK(env_->NewRandomAccessFile(filename,&files[i]));
  }
  leveldb::Slice res;
  char* scratch = nullptr;
  printf("Random: read data\n");
  ASSERT_LEVELDB_OK(files[0]->Read(0,wdata.size(),&res,scratch));
  std::cout << res.data() << std::endl;
  ASSERT_LEVELDB_OK(files[1]->Read(wdata.size(),4096,&res,scratch));
  DISPLAY_DATA(reinterpret_cast<const unsigned char*>(res.data()), res.size());
  for(int i=0; i<4; i++){
    ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
  }
  ASSERT_LEVELDB_OK(files[2]->Read(0,wdata.size(),&res,scratch));
  std::cout << res.data() << std::endl;
  ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
}

TEST_F(EnvPlogTest, TestOnRenameFile){
  std::string srcf = std::to_string(FILE4);
  std::string dstf = std::to_string(FILE5);
  std::string wdata = "RenameFile01234567890";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_,wdata.data(),srcf));
  leveldb::SequentialFile* file;
  ASSERT_LEVELDB_OK(env_->NewSequentialFile(srcf,&file));
  leveldb::Slice res;
  char* scratch = nullptr;
  ASSERT_LEVELDB_OK(file->Read(wdata.size(),&res,scratch));
  std::cout << "TestOnRenameFile read src: " << scratch << std::endl;
  ASSERT_LEVELDB_OK(env_->RenameFile(srcf,dstf));
  ASSERT_LEVELDB_OK(env_->NewSequentialFile(dstf,&file));
  ASSERT_LEVELDB_OK(file->Read(wdata.size(),&res,scratch));
  std::cout << "TestOnRenameFile read dst: " << scratch << std::endl;
}

}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}




