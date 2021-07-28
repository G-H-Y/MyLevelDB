TARGET ?= plog_fabric_demo

CC      ?= gcc

INCLUDE += -I. -I$(CURDIR)/../include
CFLAGS += $(INCLUDE) -O0 -g

LDFLAGS += -L$(CURDIR)/../lib

LDFLAGS += -Wl,--allow-shlib-undefined
LDFLAGS += -lrte_eal -lplog_fabric -lplog_fabric_init

SOURCE += $(wildcard *.c)
OBJS   := $(patsubst %.c,%.o,$(SOURCE))

.PHONY : all clean $(TARGET)

all : $(TARGET)

clean :
	rm -fr $(OBJS)
	rm -fr $(TARGET)

$(OBJS):%o:%c
	$(CC) -c $< -o $@ $(CFLAGS)

$(TARGET) : $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)
