/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "plog_fabric.h"
#include "stdbool.h"

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

static inline void plog_status_init(struct status *status)
    {
  status->done = PLOG_FABRIC_TESTING;
  status->status = PLOG_FABRIC_TEST_STATUS_TIMEOUT;
    }

    static inline int plog_cmd_done(struct status *status)
        {
  return status->done == PLOG_FABRIC_TEST_DONE;
        }

        static inline int plog_status_success(struct status *status)
            {
  return status->status == PLOG_FABRIC_TEST_STATUS_SUCCESS;
            }

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

            static inline void show_plog_disk(struct plog_disk *disk)
                {
  fprintf(stdout,
          "traddr : %s, trsvcid : %s, esn : %s, nsid : %d, dstnid : %ld\n",
          disk->traddr,
          disk->trsvcid,
          disk->esn,
          disk->nsid,
          disk->dst_nid);
                }

                static int init_plog_disk(struct plog_disk *pdisk, const char *esn, const char *bdf, uint32_t nsid)
                    {
  strcpy(pdisk->esn, esn);
  strcpy(pdisk->trsvcid, bdf);
  strcpy(pdisk->traddr, PLOG_INVALID_IP);
  pdisk->nsid = nsid;
  show_plog_disk(pdisk);
  return 0;
                    }

#define PLOG_CTX_TIMEOUT_MS (12 * 1000);
static int init_plog_io_ctx(struct plog_io_ctx *ctx, plog_usercb_func cb, void *arg)
    {
  ctx->cb = cb;
  ctx->cb_arg = arg;
  ctx->timeout_ms = PLOG_CTX_TIMEOUT_MS;
  return 0;
    }

#define PLOG_RW_MIN_GRLTY 16 /* TODO : 盘接入时具体值从盘获取保存到NS表中，当前先固定值 */
#define PLOG_PAGE_SIZE 4096
#define PLOG_DIF_AREA_SPACE 64

static void gen_write_data(void *data, int data_len)
{
  memset(data, 0xAA, data_len);
}

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

static inline uint32_t plog_calc_rw_len(uint32_t data_len, uint16_t gran)
{
  if (data_len == 0) {
    return 0;
  }

  return ((data_len + gran - 1) / gran - 1);  /* 0 base value, 向上取整再减一 */
}

static uint32_t plog_getattr_test(int fd, int plog_id){
  struct plog_io_ctx ctx = {0};
  struct plog_attr_table attr = {0};
  struct plog_param plog_param = {0};

  init_plog_io_ctx(&ctx, test_done, &g_status);

  plog_param.plog_id = plog_id;

  plog_status_init(&g_status);
  plog_get_attr(fd, &plog_param, &attr, &ctx);
  while (!plog_cmd_done(&g_status)) {
    plog_process_completions(fd);
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("get attr failed : %d\n", g_status.status);
    return -1;
  }
  printf("get written_size : %u\n", attr.written_size);
  return attr.written_size;
}

static void plog_append_buff_test(int fd, uint32_t len, uint32_t offset, int plog_id, bool is_onebuf){
  struct plog_io_ctx ctx = {0};
  struct plog_rw_param rw_info = {0};
  plog_drv_buf_list_t plog_buf;
  void* data = NULL;

  init_plog_io_ctx(&ctx, test_done, &g_status);

  rw_info.offset = offset / PLOG_RW_MIN_GRLTY;
  rw_info.length = plog_calc_rw_len(len, PLOG_RW_MIN_GRLTY);
  rw_info.plog_param.plog_id = plog_id;
  rw_info.plog_param.access_id = 0;
  rw_info.plog_param.pg_version = 0;
  rw_info.opt = PLOG_RW_OPT_APPEND;

  uint32_t dif_len = PLOG_DIF_AREA_SPACE;
  uint32_t data_len = (rw_info.length+1)*PLOG_RW_MIN_GRLTY;
  uint32_t data_offset = rw_info.offset*PLOG_RW_MIN_GRLTY;
  bool is_unaligned = (data_offset % PLOG_PAGE_SIZE);
  uint32_t curpage_rmn_len = PLOG_PAGE_SIZE - (data_offset % PLOG_PAGE_SIZE);
  uint32_t page_rmn_len = is_unaligned ? (data_len>curpage_rmn_len ? curpage_rmn_len : data_len) : 0;
  //bool is_crosspages = (is_unaligned&&(data_len>page_rmn_len)) || ((!is_unaligned)&&(data_len>PLOG_PAGE_SIZE));
  uint32_t data_rmn_len = data_len - page_rmn_len;

  int bufs = (is_unaligned) + (data_rmn_len/PLOG_PAGE_SIZE) + (data_rmn_len%PLOG_PAGE_SIZE?1:0);

  plog_buf.buffers = (plog_drv_buf_t *)calloc(bufs, sizeof(plog_drv_buf_t));
  if (plog_buf.buffers == NULL) {
    return;
  }

  data = rte_malloc(NULL, data_len + dif_len*bufs, PLOG_RW_MIN_GRLTY);
  if (data == NULL) {
    printf("rte_malloc failed\n");
    return;
  }
  uint32_t remain = data_len;
  for(int i=0; i<bufs && remain>0; i++){
    if(i==0 && is_unaligned){
      memset(data,0xFF,page_rmn_len);
      remain = data_len - page_rmn_len;
      printf("memset 0x%x bytes start from 0x%lx to 0x%X\n",page_rmn_len,data-data,0xFF);
    }else{
      void* bstart = data + (page_rmn_len+dif_len)*is_unaligned + (PLOG_PAGE_SIZE+dif_len)*(i-is_unaligned);
      if(remain >= PLOG_PAGE_SIZE){
        memset(bstart,0xAA+0x11*i,PLOG_PAGE_SIZE);
        remain -= PLOG_PAGE_SIZE;
        printf("memset 0x%x bytes start from 0x%lx to 0x%X\n",PLOG_PAGE_SIZE,bstart-data,0xAA+0x11*i);
      }else{
        memset(bstart,0xAA+0x11*i,remain);
        printf("memset 0x%x bytes start from 0x%lx to 0x%X\n",remain,bstart-data,0xAA+0x11*i);
        remain  = 0;
      }
    }
  }
  if(is_onebuf){
    plog_buf.cnt = 1;
    plog_buf.buffers[0].buf = data;
    plog_buf.buffers[0].len = data_len + dif_len*bufs;
  }else{
    plog_buf.cnt = bufs;
    uint32_t remain = data_len;
    for(int i=0; i<bufs && remain>0; i++){
      if(i==0 && is_unaligned){
        plog_buf.buffers[0].buf = data;
        plog_buf.buffers[0].len = page_rmn_len + dif_len;
        remain = data_len - page_rmn_len;
      }else{
        plog_buf.buffers[i].buf = data + (page_rmn_len+dif_len)*is_unaligned + (PLOG_PAGE_SIZE+dif_len)*(i-is_unaligned);
        if(remain >= PLOG_PAGE_SIZE){
          plog_buf.buffers[i].len = PLOG_PAGE_SIZE + dif_len;
          remain -= PLOG_PAGE_SIZE;
        }else{
          plog_buf.buffers[i].len = remain + dif_len;
          remain  = 0;
        }
      }
    }
  }

  plog_status_init(&g_status);
  plog_append_buff(fd, &rw_info, &plog_buf, NULL, &ctx);
  while (!plog_cmd_done(&g_status)) {
    plog_process_completions(fd);
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("plog_append_sgl failed : %d\n", g_status.status);
    return;
  }
  printf("append 0x%x bytes data start from offset 0x%x:\n", data_len, offset);
  DISPLAY_DATA(data, data_len + dif_len*bufs);
  sleep(1);
  rte_free(data);
}

static void plog_append_buff_test1(int fd, uint32_t len, uint32_t offset, int plog_id){
  //len = 4096*i+4096+4096+4096*i , i>=2
  //offset = 0
  int str_pagenum = 2;
  int end_pagenum = 4;
  struct plog_io_ctx ctx = {0};
  struct plog_rw_param rw_info = {0};
  plog_drv_buf_list_t plog_buf;
  void* data = NULL;

  init_plog_io_ctx(&ctx, test_done, &g_status);

  rw_info.offset = offset / PLOG_RW_MIN_GRLTY;
  rw_info.length = plog_calc_rw_len(len, PLOG_RW_MIN_GRLTY);
  rw_info.plog_param.plog_id = plog_id;
  rw_info.plog_param.access_id = 0;
  rw_info.plog_param.pg_version = 0;
  rw_info.opt = PLOG_RW_OPT_APPEND;

  uint32_t dif_len = PLOG_DIF_AREA_SPACE;
  int bufs = len/PLOG_PAGE_SIZE;
  int buf_cnt = bufs - (str_pagenum+end_pagenum-2);

  plog_buf.buffers = (plog_drv_buf_t *)calloc(bufs, sizeof(plog_drv_buf_t));
  if (plog_buf.buffers == NULL) {
    return;
  }

  data = rte_malloc(NULL, len + dif_len*bufs, PLOG_RW_MIN_GRLTY);
  if (data == NULL) {
    printf("rte_malloc failed\n");
    return;
  }
  for(int i=0; i<bufs; i++){
    memset(data+(PLOG_PAGE_SIZE+dif_len)*i, 0xAA+0X11*i, PLOG_PAGE_SIZE);
  }

  plog_buf.cnt = buf_cnt;
  void* bstart = data;
  for(int i=0; i<buf_cnt; i++){
    if(i==0){
      plog_buf.buffers[i].buf = bstart;
      plog_buf.buffers[i].len = (PLOG_PAGE_SIZE+dif_len)*str_pagenum;
      bstart = data+(PLOG_PAGE_SIZE+dif_len)*str_pagenum;
    }else if(i==buf_cnt-1){
      plog_buf.buffers[i].buf = bstart;
      plog_buf.buffers[i].len = (PLOG_PAGE_SIZE+dif_len)*end_pagenum;
      bstart = data+(PLOG_PAGE_SIZE+dif_len)*end_pagenum;
    }else{
      plog_buf.buffers[i].buf = bstart;
      plog_buf.buffers[i].len = (PLOG_PAGE_SIZE+dif_len);
      bstart = data+(PLOG_PAGE_SIZE+dif_len);
    }
  }

  plog_status_init(&g_status);
  plog_append_buff(fd, &rw_info, &plog_buf, NULL, &ctx);
  while (!plog_cmd_done(&g_status)) {
    plog_process_completions(fd);
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("plog_append_sgl failed : %d\n", g_status.status);
    return;
  }
  printf("append 0x%x bytes data start from offset 0x%x:\n", len, offset);
  DISPLAY_DATA(data, len + dif_len*bufs);
  sleep(1);
  rte_free(data);
}

static void plog_read_test(int fd, uint32_t len, uint32_t offset, int plog_id){
  // len is the actual size of request data
  // data_len is len aligned with 16 bytes
  struct plog_io_ctx ctx = {0};
  struct plog_rw_param rw_info = {0};
  plog_drv_buf_list_t plog_buf;
  void* data = NULL;

  init_plog_io_ctx(&ctx, test_done, &g_status);

  rw_info.offset = offset/PLOG_RW_MIN_GRLTY;
  rw_info.length = plog_calc_rw_len(len, PLOG_RW_MIN_GRLTY);
  rw_info.plog_param.plog_id = plog_id;
  rw_info.plog_param.access_id = 0;
  rw_info.plog_param.pg_version = 0;
  rw_info.opt = PLOG_RW_OPT_READ;

  uint32_t dif_len = PLOG_DIF_AREA_SPACE;
  uint32_t data_len = (rw_info.length+1)*PLOG_RW_MIN_GRLTY;
  uint32_t data_offset = rw_info.offset*PLOG_RW_MIN_GRLTY;
  bool is_unaligned = (data_offset % PLOG_PAGE_SIZE);
  uint32_t curpage_rmn_len = PLOG_PAGE_SIZE - (data_offset % PLOG_PAGE_SIZE);
  uint32_t page_rmn_len = is_unaligned ? (data_len>curpage_rmn_len ? curpage_rmn_len : data_len) : 0;
  //bool is_crosspages = (is_unaligned&&(data_len>page_rmn_len)) || ((!is_unaligned)&&(data_len>PLOG_PAGE_SIZE));
  uint32_t data_rmn_len = data_len - page_rmn_len;
  printf("----------------------start to read--------------------\n");
  printf("data_len : 0x%x, page_rmn_len : 0x%x, data_rmn_len : 0x%x\n", data_len, page_rmn_len, data_rmn_len);

  int bufs = (is_unaligned) + (data_rmn_len/PLOG_PAGE_SIZE) + (data_rmn_len%PLOG_PAGE_SIZE?1:0);

  plog_buf.buffers = (plog_drv_buf_t *)calloc(bufs, sizeof(plog_drv_buf_t));
  if (plog_buf.buffers == NULL) {
    return;
  }

  data = rte_malloc(NULL, data_len + dif_len*bufs, PLOG_RW_MIN_GRLTY);
  if (data == NULL) {
    printf("rte_malloc failed\n");
    return;
  }

  plog_buf.cnt = bufs;
  uint32_t remain = data_len;
  for(int i=0; i<bufs && remain>0; i++){
    if(i==0 && is_unaligned){
      plog_buf.buffers[0].buf = data;
      plog_buf.buffers[0].len = page_rmn_len + dif_len;
      remain = data_len - page_rmn_len;
    }else{
      plog_buf.buffers[i].buf = data + (page_rmn_len+dif_len)*is_unaligned + (PLOG_PAGE_SIZE+dif_len)*(i-is_unaligned);
      if(remain >= PLOG_PAGE_SIZE){
        plog_buf.buffers[i].len = PLOG_PAGE_SIZE + dif_len;
        remain -= PLOG_PAGE_SIZE;
      }else{
        plog_buf.buffers[i].len = remain + dif_len;
        remain  = 0;
      }
    }
  }

  plog_status_init(&g_status);
  plog_read_buff(fd, &rw_info, &plog_buf, NULL,&ctx);
  while (!plog_cmd_done(&g_status)) {
    plog_process_completions(fd);
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("plog_read_sgl failed : %d\n", g_status.status);
    return;
  }
  printf("read 0x%x bytes data start from offset 0x%x:\n", len, offset);
  DISPLAY_DATA(data, data_len + dif_len*bufs);
  rte_free(data);
}


static void plog_test(const char *esn, const char *bdf, int plog_id)
{
  int fd;
  struct plog_disk pdisk = {0};
  struct plog_io_ctx ctx = {0};
  struct plog_attr_table attr = {0};
  struct plog_param plog_param = {0};
  uint32_t written_size;
  void *data = NULL;

  init_plog_disk(&pdisk, esn, bdf, PLOG_DEFAULT_NS);
  init_plog_io_ctx(&ctx, test_done, &g_status);

  /* plog_create_ctrlr test */
  plog_status_init(&g_status);
  plog_create_ctrlr(&pdisk, &ctx);
  while (!plog_cmd_done(&g_status)) {
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("Create ctrlr failed : %d\n", g_status.status);
    return;
  }
  /* plog_create_ctrlr done */

  /* plog_open test */
  fd = plog_open(&pdisk);
  if (fd < 1) {
    printf("plog_open failed : %d\n", fd);
    return;
  }
  /* plog_open done */

  /* plog_create test */
  attr.create_size = 1024;
  plog_param.plog_id = plog_id;
  plog_status_init(&g_status);
  plog_create(fd, &plog_param, &attr, &ctx);
  while (!plog_cmd_done(&g_status)) {
    plog_process_completions(fd);
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("[warning] plog_create failed : %d\n", g_status.status);
  }
  /* plog_create done */

  /* plog_write_buff test */
  written_size = plog_getattr_test(fd, plog_id);
  plog_append_buff_test1(fd, 4096*6, written_size, plog_id);
  /*
  written_size = plog_getattr_test(fd, plog_id);
  plog_append_buff_test(fd, 1024, written_size, plog_id, 0);
  written_size = plog_getattr_test(fd, plog_id);
  plog_append_buff_test(fd, 4096+1024, written_size, plog_id, 0);
  written_size = plog_getattr_test(fd, plog_id);
  plog_append_buff_test(fd, 4096*2+2048, written_size, plog_id, 0);*/
  //plog_append_buff_test(fd, 4096*6, written_size, plog_id, 1);
  //plog_append_buff_test1(fd, 4096*6, written_size, plog_id);
  //written_size = plog_getattr_test(fd, plog_id);
  //plog_append_buff_test(fd, 4096+1024, written_size, plog_id, 0);
  //written_size = plog_getattr_test(fd, plog_id);
  //plog_append_buff_test(fd, 4096+1024, written_size, plog_id, 0);

  //written_size = plog_getattr_test(fd, plog_id);
  //plog_append_buff_test(fd, 1024, written_size, plog_id, 1);

  //written_size = plog_getattr_test(fd, plog_id);
  //plog_append_buff_test(fd, 3072, written_size, plog_id, 1);
  /* plog_write_buff done */

  /* plog_read_buff test */
  written_size = plog_getattr_test(fd, plog_id);
  plog_read_test(fd, written_size, 0, plog_id);
  /*
  for(int i=0; i<6; i++){
    plog_read_test(fd, PLOG_PAGE_SIZE, PLOG_PAGE_SIZE*i, plog_id);
  }
   */
  //plog_read_test(fd, 1536, 2048,plog_id);
  /* plog_read_buff done */

  /* plog_seal test */
  plog_status_init(&g_status);
  /* creat plog后为open状态，并且写入内容，不能字节delete plog，需要先把状态置为seal */
  plog_seal(fd, &plog_param, 0, 3, &ctx);
  while (!plog_cmd_done(&g_status)) {
    plog_process_completions(fd);
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("[warning] plog_seal failed : %d\n", g_status.status);
  }
  /* plog_seal done */

  /* plog_delete test */
  plog_status_init(&g_status);
  plog_delete(fd, &plog_param, &ctx);
  while (!plog_cmd_done(&g_status)) {
    plog_process_completions(fd);
    usleep(1000);
  }
  if (!plog_status_success(&g_status)) {
    printf("[warning] plog_delete failed : %d\n", g_status.status);
  }
  /* plog_delete done */

  rte_free(data);
  plog_close(fd);
  plog_delete_ctrlr(&pdisk);
}

/* ******************************************************************************
函数名称: main
功能说明: ptest_server主函数
****************************************************************************** */
int main(int argc, char **argv)
{
  const char *esn = argv[1];
  const char *bdf = argv[2];
  const char *plog_id_str = argv[3];

  if (esn == NULL || bdf == NULL || plog_id_str == NULL) {
    printf("esn or bdf is NULL, or plog id unknown.\n");
    return 1;
  }

  plog_fabric_init();
  plog_test(esn, bdf, atoi(plog_id_str));
  plog_fabric_fini();


  return 0;
}
