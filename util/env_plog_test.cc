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

struct para{
  int num;
  leveldb::WritableFile* file;
};

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
  std::string filename;
  std::string wdata;
  filename = "write1.txt";
  wdata = "W0123456789";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_, wdata.data(), filename));
  leveldb::WritableFile* file = nullptr;
  ASSERT_LEVELDB_OK(env_->NewWritableFile(filename, &file));
  wdata = "Wappend123456";
  Slice sdata(wdata.data(),wdata.size());
  ASSERT_LEVELDB_OK(file->Append(sdata));
  ASSERT_LEVELDB_OK(file->Flush());
  ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
  //test write MANIFEST
  pthread_t tid[6];
  for(int i=0; i<6; i++){
    if(i!=5){
      std::string pfilename = std::to_string(i)+"testonwM";
      struct para p;
      p.num = i;
      ASSERT_LEVELDB_OK(env_->NewWritableFile(filename,&p.file));
      pthread_create(&tid[i],NULL,
                               reinterpret_cast<void* (*)(void*)>(EnvPlogTest::write_file),&p);
    }else{
      std::string pfilename = "MANIFEST001";
      leveldb::WritableFile* pfile;
      ASSERT_LEVELDB_OK(env_->NewWritableFile(filename,&pfile));
      pthread_create(&tid[i],NULL,
          reinterpret_cast<void* (*)(void*)>(EnvPlogTest::write_manifest),&pfile);
    }
  }

  for(int i=0; i<6; i++){
    pthread_join(tid[i],NULL);
  }
}

TEST_F(EnvPlogTest,TestOnSequentialFile){
  std::string filename = "test_on_sequential.txt";
  std::string wdata = "S01234567890";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_,wdata.data(),filename));
  leveldb::SequentialFile* file = nullptr;
  ASSERT_LEVELDB_OK(env_->NewSequentialFile(filename,&file));
  Slice* res = nullptr;
  char* scratch = nullptr;
  ASSERT_LEVELDB_OK(file->Read(wdata.size(),res,scratch));
  std::cout << "TestOnSequentialFile: " << scratch << std::endl;
  delete file;
  ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
}

TEST_F(EnvPlogTest, TestOnRandomAccessFile){
  std::string filename = "test_on_random.txt";
  std::string wdata = "R01234567890";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_,wdata.data(),filename));
  leveldb::RandomAccessFile* files[5]={nullptr};
  for(int i=0; i<5; i++){
    ASSERT_LEVELDB_OK(env_->NewRandomAccessFile(filename,&files[i]));
  }
  Slice* res = nullptr;
  char* scratch = nullptr;
  ASSERT_LEVELDB_OK(files[0]->Read(0,wdata.size(),res,scratch));
  std::cout << "TestOnRandomAccesslFile: " << scratch << std::endl;
  for(int i=0; i<4; i++){
    ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
  }
  memset(scratch,0x0,wdata.size());
  ASSERT_LEVELDB_OK(files[0]->Read(0,wdata.size(),res,scratch));
  std::cout << "TestOnRandomAccesslFile: " << scratch << std::endl;
  for(int i=0; i<4; i++){
    delete files[i];
  }
  ASSERT_LEVELDB_OK(env_->RemoveFile(filename));
}

TEST_F(EnvPlogTest, TestOnRenameFile){
  std::string srcf = "src.txt";
  std::string dstf = "dstf.txt";
  std::string wdata = "RenameFile01234567890";
  ASSERT_LEVELDB_OK(WriteStringToFile(env_,wdata.data(),srcf));
  leveldb::SequentialFile* file;
  ASSERT_LEVELDB_OK(env_->NewSequentialFile(srcf,&file));
  Slice* res = nullptr;
  char* scratch = nullptr;
  ASSERT_LEVELDB_OK(file->Read(wdata.size(),res,scratch));
  std::cout << "TestOnRenameFile read src: " << scratch << std::endl;
  ASSERT_LEVELDB_OK(env_->RenameFile(srcf,dstf));
  ASSERT_LEVELDB_OK(env_->NewSequentialFile(dstf,&file));
  ASSERT_LEVELDB_OK(file->Read(wdata.size(),res,scratch));
  std::cout << "TestOnRenameFile read dst: " << scratch << std::endl;
}

}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}




