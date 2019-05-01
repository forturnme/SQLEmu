#include <iostream>

// MEM maneuvering
#include "extmem.h"
#include "extmem.c"

// B+ Tree implementing
#include "predefined.h"
#include "bpt.h"
#include "bpt.cc"

#include "math.h"
#include "assert.h"

#define RELATION_R 0
#define RELATION_S 1

#define REL_START_END(src, firstBlkNum, lastBlkNum) switch(src){case 0:firstBlkNum = 1;lastBlkNum = 16;break;case 1:firstBlkNum = 20;lastBlkNum = 51;break;default:return;}
#define REL_FIRST_VALUE_VALID(src, value) switch(src){case 0:if(value<1||value>40)return;break;case 1:if(value<20||value>60)return;break;default:return;}

Buffer buf; /* A buffer */

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

class writeBufferBlock
{
    // 描述了一个位于缓冲区bufAddr，地址blk,空闲区7,块编号1000的写缓存块
public:
    Buffer* bufAddr = NULL;
    unsigned char *blk = NULL;
    int freeSpace = 7;
    int blockCode = 1000;
    writeBufferBlock(Buffer* buff, int startCode);
    ~writeBufferBlock();
    void writeOneTuple(const unsigned char* src);
};

writeBufferBlock::writeBufferBlock(Buffer *buff, int startCode) {
    // 在内存区buff内开辟一块新写入缓存，缓存块编号从startCode开始
    this->bufAddr = buff;
    this->blk = getNewBlockInBuffer(buff);
    bzero(this->blk, 64*sizeof(char));
    this->blockCode = startCode;
    this->freeSpace = 7;
}

writeBufferBlock::~writeBufferBlock(){
    // 被释放之前先把自己写到盘里，下一块地址填入0
    if(this->freeSpace!=7){
        setBlkNextAddr(this->blk, 0);
        putBlockFromBufToDisk(this->blk, this->blockCode, this->bufAddr);
    }
}

void writeBufferBlock::writeOneTuple(const unsigned char* src){
    // 向缓存写进一个元组，8byte
    // 若空余的空间没有，则先把自己写进磁盘，然后申请新的区域，更新写入缓存，再开始写入
    if(this->freeSpace==0){
        // 开始转写进磁盘
        setBlkNextAddr(this->blk, this->blockCode+1);
        putBlockFromBufToDisk(this->blk, this->blockCode, this->bufAddr);
        this->blockCode++;
        this->blk = getNewBlockInBuffer(this->bufAddr);
        bzero(this->blk, 64*sizeof(char));
        this->freeSpace = 7;
    }
    memcpy(this->blk+8*(7-freeSpace), src, 8* sizeof(char));
    this->freeSpace--;
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
    std::cout << "[BLK] " << getBlkPosInMem(blk) << " /" << buf.numAllBlk << " blocks in memory - " << (isBlkUsed(blk)?"USED":"IDLE") << std::endl;
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
    return fourCharToInt((char*)blk + 8 * n + y);
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

void selectFromRel_linear(int value, int src, int startBlock){
    // 从r或s选出前一个属性为对应值的元组，线性选择，naive
    // 结果存在从startBlock开始的区域
    int firstBlkNum = 0, lastBlkNum = 0;
    REL_START_END(src,firstBlkNum,lastBlkNum)
    REL_FIRST_VALUE_VALID(src,value)
    unsigned char *blk;
    auto *writeBlk = new writeBufferBlock(&buf, startBlock);
    for (int i = firstBlkNum; i <= lastBlkNum; ++i) {
        blk = getBlockFromDiskToBuf(i, &buf);
        for (int j = 0; j < 7; ++j) {
            if(getNthTupleY(blk, j, 0)==value){
                writeBlk->writeOneTuple(blk+j*8);
            }
        }
        freeBlockInBuffer(blk, &buf);
    }
    delete(writeBlk);// 删掉这个来触发最后一次写盘
}

void sortRel(int src, int startBlock){
    // 排序整个关系
    int firstBlkNum = 0, lastBlkNum = 0;
    REL_START_END(src,firstBlkNum,lastBlkNum)
    int blkCnt = lastBlkNum-firstBlkNum+1;
    unsigned char* blk[7]; // 指使用的块地址
    int segCnt = blkCnt / 7 + (blkCnt%7==0?0:1); // 段数，每段最多7块
    // 第一趟排序：块内排序
    for (int i = 0; i < segCnt; ++i) {
        // 块内排序
        int j = 0; // 可以得出本轮用到的块数
        for (; j < 7 && j + i * 7 < blkCnt; ++j) {
            blk[j] = getBlockFromDiskToBuf(j+i*7+firstBlkNum, &buf);
            sortBlock(blk[j]);
        }
        // 7路归并输出为有序块
        auto *writeBlk = new writeBufferBlock(&buf, startBlock+100+i*7);
        int blkInd[7] = {0}; // 指示每块里当前扫视到的位置
        int minNum; // 此轮最小的值
        int minIn; // 此轮最小的位置
        int currentX; // 当前扫视的值
        for (int k = 0; k < 7 * j; ++k) {
            // 此轮把7*j个元组排序完毕
            minNum = 10000;
            minIn = -1;
            for (int l = 0; l < j; l++) {
                // 依块扫视
                if(blkInd[l]==7)continue;
                currentX = getNthTupleY(blk[l], blkInd[l], 0);
                if(currentX ==0)continue; // 如果扫到空元组则不干了
                if(currentX < minNum){
                    // 更新最小值
                    minNum = currentX;
                    minIn = l;
                }
            }
            if(minIn==-1)break; // 若一轮中未检查到变更则说明归并完成
            // 写元组进输出缓冲，并维护指示
            writeBlk->writeOneTuple(blk[minIn]+8*blkInd[minIn]);
            blkInd[minIn]++;
        }
        delete(writeBlk);
        // 每轮结束后释放全部块空间
        for (int m = 0; m < j; ++m) {
            freeBlockInBuffer(blk[m], &buf);
        }
    }
    // 第二趟排序：横向归并
    int* readCandidates = (int*)malloc(blkCnt*sizeof(int)); // 决定块的读入顺序
    bzero(readCandidates, blkCnt*sizeof(int));
    for (int i = 0; i < segCnt; ++i) {
        // 首先读进来最小的segCnt块
        blk[i] = getBlockFromDiskToBuf(startBlock + 100 + i * 7, &buf);
    }
    auto *writeBlk = new writeBufferBlock(&buf, startBlock);
    int blkInd[7] = {0}; // 指示每块里当前扫视到的位置
    int changed[7] = {1,1,1,1,1,1,1}; // 指示每内存块上发生读的次数，加上i*7（内存块编号）可以指示下一装入块，初始化后已经读过一次了
    int minNum; // 此轮最小的值
    int minIn; // 此轮最小的位置
    int currentX; // 当前扫视的值
    while (true){
        // 然后边归并边读剩下的块
        minNum = 10000;
        minIn = -1;
        for (int i = 0; i < segCnt; ++i) {
            if(blkInd[i]==7){
                // 发现这个块扫描完了，则需要换一块进来
                if(changed[i]+i*7>=blkCnt||changed[i]>=7){
                    // 如果要装进的块不存在了，则跳过
                    continue;
                }
                // 释放旧块，装进新块，换块次数加一，重置位置指示，完成后正常地维护最小值
                freeBlockInBuffer(blk[i], &buf);
                blk[i] = getBlockFromDiskToBuf(changed[i]+i*7+startBlock+100, &buf);
                changed[i]++;
                blkInd[i] = 0;
            }
            currentX = getNthTupleY(blk[i], blkInd[i], 0);
            if(currentX==0)continue; // 扫描到空元组则跳过
            if(currentX < minNum){
                // 更新最小值
                minNum = currentX;
                minIn = i;
            }
        }
        if(minIn==-1)break; // 若一次扫描中未能找出最小的块，则说明排序结束
        // 写元组进输出缓冲，并维护指示
        writeBlk->writeOneTuple(blk[minIn]+8*blkInd[minIn]);
        blkInd[minIn]++;
    }
    // 完成后清掉整个buffer
    delete(writeBlk);
    for (int m = 0; m < segCnt; ++m) {
        freeBlockInBuffer(blk[m], &buf);
    }
}

void binarySearch(int val, int relStart, int relEnd, int outputStartBlock){
    // 二分查找，输入欲查找的值，排好序的关系的起始块号和终止块号，输出结果的起始块号
    // 将会把结果写到盘里
    int max, min;
    unsigned char *maxBlk, *minBlk;
    auto writeBlk = new writeBufferBlock(&buf, outputStartBlock);
    for (int i = relStart; i < relEnd; ++i) {
        // 打开两头块，分别取最小
        minBlk = getBlockFromDiskToBuf(relEnd, &buf);
        if(getNthTupleY(minBlk, 0, 0) > val){
            // 若最小的块也比它大，则认为找不到，开始做清除工作
            freeBlockInBuffer(minBlk, &buf);
            delete(writeBlk);
            return;
        }
        maxBlk = getBlockFromDiskToBuf(relStart, &buf);
        if(getNthTupleY(maxBlk, 0, 0) <= val){
            // 如果最大块里最小的小于或等于val，则先查找这块里的内容
            for (int j = 0; j < 7; ++j) {
                if(getNthTupleY(maxBlk, j, 0)==0){
                    break; // 如果获得空块则停止
                }
                if(getNthTupleY(maxBlk, j, 0)==val){
                    writeBlk->writeOneTuple(maxBlk+8*j);
                }
            }
            delete(writeBlk);
            freeBlockInBuffer(maxBlk, &buf);
            freeBlockInBuffer(minBlk, &buf);
            return;
        }
        // 加上
        // TODO: 添加折半相关的逻辑

    }
}

int main() {
    // 以例程为脚手架
    unsigned char *blk; /* A pointer to a block */
    int i = 0;

    /* Initialize the buffer */
    if (!initBuffer(520, 64, &buf))
    {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }

    /* Read the block from the hard disk */
    blk = getBlockFromDiskToBuf(1, &buf);
    showBlock(blk);

    sortBlock(blk);
    showBlock(blk);

    freeBlockInBuffer(blk, &buf);

    selectFromRel_linear(40, RELATION_R, 1000);
    selectFromRel_linear(60, RELATION_S, 1100);

    std::cout<<"Empty Blocks: "<<buf.numFreeBlk<<std::endl;

    sortRel(RELATION_R, 1200);
    sortRel(RELATION_S, 1400);

    std::cout << '\n' << "IO's is " << buf.numIO << std::endl;
    return 0;
}
