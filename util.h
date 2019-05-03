#include <iostream>
#include "extmem.h"
#include "extmem.c"
/*一些不知道往哪放的工具函数*/

#pragma once

Buffer buf; /* A buffer */

inline void bzero(void *pt, size_t size){
    memset(pt, 0, size);
}

unsigned char* getBlockFromDiskToBuf(int blknum, Buffer* buff){
    unsigned char *blk = NULL;
    if ((blk = readBlockFromDisk(blknum, buff)) == NULL)
    {
        perror("Reading Block Failed!\n");
        exit(-1);
    }
    return blk;
}

inline void putBlockFromBufToDisk(unsigned char* blk, int blknum, Buffer* buff){
    if (writeBlockToDisk(blk, blknum, buff) != 0)
    {
        perror("Writing Block Failed!\n");
        exit(-1);
    }
    std::cout<< "Result block Written as [" << blknum << ".blk]" <<std::endl;
}

inline void setBlkNextAddr(unsigned char* blk, int nextAddr){
    itoa(nextAddr, (char*)(blk+56), 10);
}


inline int getBlkPosInMem(const unsigned char *blk){
    return (blk-buf.data-1)/buf.blkSize+1;
}

inline int isBlkUsed(const unsigned char *blk){
    return *(blk - 1) != 0;
}

void showBlock(const unsigned char *blk){
    if(!blk)
        return;
    /* Process the data in the block */
    int X = -1;
    int Y = -1;
    int addr = -1;
    char str[5];
    std::cout << "[BLK] " << getBlkPosInMem(blk) << " /" << buf.numAllBlk <<
        " blocks in memory - " << (isBlkUsed(blk)?"USED":"IDLE") << std::endl;
    int i;
    printf("      ");
    for (i = 0; i < 7; i++) //一个blk存7个元组加一个地址
    {
        for (int k = 0; k < 4; k++)
        {
            str[k] = *(blk + i*8 + k);
        }
        X = atoi(str);
        for (int k = 0; k < 4; k++)
        {
            str[k] = *(blk + i*8 + 4 + k);
        }
        Y = atoi(str);
        printf("(%d, %d) ", X, Y);
    }
    for (int k = 0; k < 4; k++)
    {
        str[k] = *(blk + i*8 + k);
    }
    addr = atoi(str);
    printf("\n      Next block in hard disk: %d \n\n", addr);
}

int fourCharToInt(const void* pt){
    // 四个ascii字节转换为int
    char conv[5];
    bzero(conv, 5*sizeof(char));
    memcpy(conv, pt, 4*sizeof(char));
    return atoi(conv);
}

inline int getNthTupleY(const void *blk, const int n, const int y){
    // 取得blk中的第n个元组里的第y项。
    // 0<= n <=6, 0<= y <=1
    return fourCharToInt((char*)blk + 8 * n + y * 4);
}

inline void swapTuples(const unsigned char* blk, int t1, int t2){
    // 交换blk中的第t1个和第t2个元组
    unsigned long long temp = 0;
    memcpy(&temp, blk+8*t1, sizeof(char)*8);
    memcpy((void*)(blk+8*t1), blk+8*t2, sizeof(char)*8);
    memcpy((void*)(blk+8*t2), &temp, sizeof(char)*8);
}

void sortBlock(const unsigned char *blk){
    // 排序一个块中的元组，按前一个属性做升序，由于只有7条目所以做冒泡排序
    for (int i = 0; i < 6; i++) {
        for (int j = 0; j < 6 - i; j++) {
            if (getNthTupleY(blk, j+1, 0) < getNthTupleY(blk, j, 0)) {
                swapTuples(blk, j, j + 1);
            }
        }
    }
}

void printIO(Buffer *buff){
    // 打印IO次数
    std::cout << '\n' << "[IO TOTAL] " << buff->numIO << std::endl;
}
