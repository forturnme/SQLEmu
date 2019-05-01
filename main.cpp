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

void selectFromRel_Binary(int val, int relStart, int relEnd, int outputStartBlock){
    // 二分查找，输入欲查找的值，排好序的关系的起始块号和终止块号，输出结果的起始块号
    // 将会把结果写到盘里
    int startToFind; // 从此块开始找
    int minN = relStart, maxN = relEnd; // 用来进行二分查找的哨兵指示
    int foo; // 这个不知道叫什么好，用来存放块的最小值
    unsigned char *maxBlk=NULL, *minBlk=NULL, *medBlk=NULL, *startToFindBlk=NULL;
    auto writeBlk = new writeBufferBlock(&buf, outputStartBlock);
    while (true) {
        if(minN == maxN){
            // 只剩一块，直接开始查找
            startToFindBlk = maxBlk!=NULL?maxBlk:(minBlk!=NULL?minBlk:getBlockFromDiskToBuf(minN, &buf));
            startToFind = minN;
            break;
        }
        // 打开两头块，分别取最小
        if(minBlk==NULL){
            minBlk = getBlockFromDiskToBuf(minN, &buf);
            foo = getNthTupleY(minBlk, 0, 0);
            if(foo > val){
                // 若最小的块也比它大，则认为找不到，开始做清除工作
                freeBlockInBuffer(minBlk, &buf);
                delete(writeBlk);
                return;
            }
            if(foo == val){
                // 若最小的值等于val，直接从最小块开始找
                startToFind = minN;
                startToFindBlk = minBlk;
                if(maxBlk!=NULL)freeBlockInBuffer(maxBlk, &buf);
                break;
            }
        }

        if(maxBlk==NULL){
            maxBlk = getBlockFromDiskToBuf(maxN, &buf);
            foo = getNthTupleY(maxBlk, 0, 0);
            if(foo < val){
                // 如果最大块里最小的小于val，则直接查找这块里的内容
                startToFind = maxN;
                startToFindBlk = maxBlk;
                freeBlockInBuffer(minBlk, &buf);
                break;
            }
            if(foo == val){
                // 如果最小值等于val，则说明应该从之前的某一块开始找
                freeBlockInBuffer(minBlk, &buf);
                for (int j = maxN; j >= minN ; --j) {
                    freeBlockInBuffer(maxBlk, &buf);
                    maxBlk = getBlockFromDiskToBuf(j, &buf);
                    if(getNthTupleY(maxBlk, 0, 0)<val){
                        startToFind = j;
                        startToFindBlk = maxBlk;
                        break;
                    }
                }
                break;
            }
        }

        // 如果都不是，那就看中间块的最小值
        medBlk = getBlockFromDiskToBuf((maxN - minN) / 2 + minN, &buf);
        foo = getNthTupleY(medBlk, 0, 0);
        if(foo>val){
            // 中间块最小的值大于val，说明下一轮应该找前面半截，所以我们改变maxN并释放maxBlk
            maxN = (maxN - minN) / 2 + minN - 1;
            freeBlockInBuffer(maxBlk, &buf);
            freeBlockInBuffer(medBlk, &buf);
            maxBlk = NULL;
            continue;
        }
        else if(foo<val){
            // 中间块最小值小于val，就要先检查最大值
            for (int i = 6; i > 0; --i) {
                foo = getNthTupleY(medBlk, i, 0);
                if(foo!=0)break;
            }
            // 最大值大于等于val，就命中这块
            if(foo>=val){
                startToFind = (maxN - minN) / 2 + minN;
                startToFindBlk = medBlk;
                if(maxN!=(maxN - minN) / 2 + minN)freeBlockInBuffer(maxBlk, &buf);
                if(minN!=(maxN - minN) / 2 + minN)freeBlockInBuffer(minBlk, &buf);
                break;
            }
            // 中间块的最大值也小于val，下一轮应从后半部分开始，故改变minN并变更minBlk
            if(minN!=(maxN - minN) / 2 + minN){
                freeBlockInBuffer(minBlk, &buf);
                minN = (maxN - minN) / 2 + minN;
            }
            minBlk = medBlk;
            continue;
        }
        else {
            // 我们不允许要找的值被截断在两个部分的情形
            // 当中间的块最小值正好等于要找的值时，我们应该向前找
            freeBlockInBuffer(maxBlk, &buf);
            freeBlockInBuffer(minBlk, &buf);
            for (int j = (maxN-minN)/2+minN-1; j >= minN ; --j) {
                freeBlockInBuffer(medBlk, &buf);
                medBlk = getBlockFromDiskToBuf(j, &buf);
                if(getNthTupleY(medBlk, 0, 0)<val){
                    startToFind = j;
                    startToFindBlk = medBlk;
                    break;
                }
            }
            break;
        }
    }
    for (int i = startToFind; i <= maxN; ++i) {
        // 遍历可能的每一块
        for (int j = 0; j < 7; ++j) {
            foo = getNthTupleY(startToFindBlk, j, 0);
            if (foo < val && foo != 0)continue;
            if (foo == val) {
                writeBlk->writeOneTuple(startToFindBlk + 8 * j);
            } else {
                delete(writeBlk);
                freeBlockInBuffer(startToFindBlk, &buf);
                return;
            }
        }
        if (i == maxN){
            // 读到最后一块，退出
            delete(writeBlk);
            freeBlockInBuffer(startToFindBlk, &buf);
            return;
        }
        // 读下一块
        freeBlockInBuffer(startToFindBlk, &buf);
        startToFindBlk = getBlockFromDiskToBuf(i + 1, &buf);
    }
}

class readBlocks{
    /* 读取内存块，专门用来读元组的。
     * 可以指定：
     *         -这个区域占有内存块的数量
     *         -能读取的块号范围
     * 需要维护：
     *         -其中的内存块和磁盘块的对应关系
     *         -当前读取到的内存块和块中的位置
     * 可以进行：
     *         -读取一条记录，然后指示向后移动
     *         -读取一条记录，指针不移动
     *         -要求更换内容，将内存中指针之前的块全部更新
     *         -持有一个R-device，可以存储某时刻自身的状态，包括内存块和磁盘的对应表，当前指针所在的块和位置，持有的内存在此时的状态
     *         -从有内容的R-device进行事象回溯
     */
public:
    int memCnt=0; // 持有的内存块数
    Buffer *buff; // 宿主
    readBlocks(int startBlk, int endBlk, int memCnt, Buffer *buff);
    ~readBlocks();
    unsigned char* getTupleSilent(); // 得到元组地址，指针不动
    void forward(); // 指针移动一个单位，若无法移动则无事发生
    unsigned char* getTuple(); // 得到元组地址，指针移动，若无法移动则不移动
    int getValSilent(int which); // 得到元组中的第which个元素
    int getVal(int which); // 得到之后还移动
    bool doSnapshot(); // 存储当前的状态
    bool recall(); // 还原此前的状态
    bool refresh(); // 更换指针所在块前面的块
private:
    int startBlock=0; // 开始的磁盘块编号
    int endBlock=0; // 最后一个磁盘块的编号
    int qFront=0; // 下面两个循环队列的队首号
    int qEnd=0; // 队尾号
    bool qForward(); // 队列向前生长
    inline void qShrink(); // 队列向前收缩
    inline bool isFront(int n); // 判断n是不是在队列头
    inline unsigned char*& getNthBlock(int n); // 获得队列中第n个地址
    inline int& getNthNumber(int n); // 获取队列中第n个块号
    inline int getNthNumberVal(int n); // 获取队列中第n个块号
    inline bool toNextBlock(); // 使指针移到下个块，不能移动则返回false
    int qLength=0; // 队列中拥有的非空块个数
    unsigned char** memBlocks=NULL; // 持有的内存块地址列表，是循环队列
    int* blkNums=NULL; // 对应的磁盘块编号，是循环队列
    int block=0; // 现在扫描到了何块
    int tuple=0; // 现在扫描到了哪个元组
    struct {
        bool hasReflect= false; // 有没有使用R装置储存事象
//        unsigned char** memBlocks=NULL; // 持有的内存块地址列表
        int qFront=0; // 下面两个循环队列的队首号，等于新元素将插入的位置?
        int qEnd=0; // 队尾号
        int qLength=0; // 队列中拥有的非空块个数
        int* blkNums=NULL; // 对应的磁盘块编号
        int block=0; // 现在扫描到了何块
        int tuple=0; // 现在扫描到了哪个元组
    }R_device; // 存储事象供回溯使用
    inline bool full(); // 判断队列是否满
    inline bool empty(); // 判断队列是否空
//    int* generateSequentalIndices(); // 为队列生成正常顺序的访问下标列表
    bool loadBlkFromDisc(int blkNum); // 装载一块进入磁盘
    bool removeLastBlk(); // 移除储存的最后一块
};

readBlocks::readBlocks(int startBlk, int endBlk, int memCnt, Buffer *buff) {
    // 生成读取区域。输入开始块编号，结束块编号，持有内存块数，对应缓冲区
    this->buff = buff;
    this->memCnt = memCnt;
    this->blkNums = (int*)malloc(sizeof(int)*memCnt);
    this->memBlocks = (unsigned char**)malloc(sizeof(unsigned char*)*memCnt);
    this->R_device.blkNums = (int*)malloc(sizeof(int)*memCnt);
    bzero(this->memBlocks, sizeof(unsigned char*)*memCnt);
    bzero(this->R_device.blkNums, sizeof(int)*memCnt);
    bzero(this->blkNums, sizeof(int)*memCnt);
//    this->R_device.memBlocks = (unsigned char**)malloc(sizeof(unsigned char*)*memCnt);
    this->startBlock = startBlk;
    this->endBlock = endBlk;
    // 装入第一块来开张
    this->loadBlkFromDisc(this->startBlock);
}

bool readBlocks::doSnapshot() {
    // 做快照
//    memcpy(this->R_device.memBlocks, this->memBlocks, sizeof(unsigned char*)*memCnt);
    memcpy(this->R_device.blkNums, this->blkNums, sizeof(int)*memCnt);
    this->R_device.block = this->block;
    this->R_device.qEnd = this->qEnd;
    this->R_device.qFront = this->qFront;
    this->R_device.qLength = this->qLength;
    this->R_device.tuple = this->tuple;
    this->R_device.hasReflect = true;
    return true;
}

inline bool readBlocks::toNextBlock() {
    // 块指针后移1个
    if(this->isFront(this->block)){
        int blkNumNow = this->getNthNumberVal(this->block);
        if(blkNumNow < this->endBlock){
            if(full())this->qShrink();
            this->loadBlkFromDisc(blkNumNow+1);
            return true;
        } else{
            return false;
        }
    }
    this->block++;
    return true;
}

bool readBlocks::qForward() {
    // 队首前进
    if(this->full()){
        perror("ERROR 530");
        return false;
    }
    this->qFront++;
    this->qLength++;
    return true;
}

inline void readBlocks::qShrink() {
    // 收缩队列
    if(this->qFront>this->qEnd){
        this->qEnd++;
        this->qLength--;
    }
}

inline int& readBlocks::getNthNumber(int n) {
    // 获得第n个块编号
    return this->blkNums[(n+this->qEnd)%this->memCnt];
}

inline int readBlocks::getNthNumberVal(int n) {
    // 获得第n个块编号
    return this->blkNums[(n+this->qEnd)%this->memCnt];
}

inline unsigned char*& readBlocks::getNthBlock(int n) {
    // 获得第n块
    return this->memBlocks[(n+this->qEnd)%this->memCnt];
}

inline bool readBlocks::full(){
    // 判断是否满
    if(this->memCnt==1)return false;
    return this->qFront - this->qEnd >= this->memCnt;
}

inline bool readBlocks::empty(){
    // 判断是否空
    return this->qFront - this->qEnd > 0;
}

bool readBlocks::loadBlkFromDisc(int blkNum) {
    // 装载第blkNum块进磁盘，不影响指针，但是不碍事
    if(!this->qForward())return false;
    // 先推进队列
    // 然后装入blkNum块
    unsigned char*& blk = this->getNthBlock(this->qFront-this->qEnd);
    int& blkn = this->getNthNumber(this->qFront-this->qEnd);
    blk = getBlockFromDiskToBuf(blkNum, this->buff);
    blkn = blkNum;
    return true;
}

bool readBlocks::removeLastBlk() {
    // 删除队尾的一块内存，同时维护指针，保证指针不移动
    if(this->empty())return false;
//    if(this->block==0)perror("Read Block is empty now");
    // 先删队尾
    freeBlockInBuffer(this->getNthBlock(0), this->buff);
    // 再缩队列
    this->qShrink();
    this->block--; // 维护块指针
    return true;
}

inline bool readBlocks::isFront(int n) {
    // 判断是否已经到队列头
    return this->qFront - this->qEnd <= n;
}

void readBlocks::forward() {
    // 前移指针一个元组
    if(this->tuple==6){
        // 若此块满，则要前进一块
        if(this->toNextBlock())this->tuple=0;
    }
    else{
        // 若没满则直接加
        if(getNthTupleY(this->getNthBlock(this->block), this->tuple+1, 0)!=0){
            this->tuple++;
        }
    }
}

unsigned char* readBlocks::getTupleSilent() {
    // 获得当前元组的地址
    return (this->getNthBlock(this->block)+this->tuple*8);
}

unsigned char* readBlocks::getTuple() {
    // 获得当前元组地址并让地址前进
    unsigned char* t = this->getTupleSilent();
    this->forward();
    return t;
}

int readBlocks::getValSilent(int which){
    // 获得元组的第n个值
    return getNthTupleY(getNthBlock(this->block), this->tuple, which);
}

int readBlocks::getVal(int which) {
    // 获得元组的第n个值，然后指针前进
    int res = this->getValSilent(which);
    this->forward();
    return res;
}

bool readBlocks::refresh() {
    // 刷新内存区域，尽可能地更换指针之前的块
    int blkToLoad;
    for (int i = 0; i < this->memCnt-this->qLength; ++i) {
        // 首先把剩下的区域填充完
        blkToLoad = this->getNthNumber(this->qFront-this->qEnd)+1;
        if(blkToLoad>this->endBlock)break;
        this->loadBlkFromDisc(blkToLoad);
    }
    for (int i = 0; i < this->memCnt; ++i) {
        // 然后开始尽可能替换块
        if(this->block==0)break;
        blkToLoad = this->getNthNumber(this->qFront-this->qEnd)+1;
        if(blkToLoad>this->endBlock)break;
        this->removeLastBlk();
        this->loadBlkFromDisc(blkToLoad);
    }
    return true;
}

bool readBlocks::recall() {
    // 还原至储存的事象，包含将内存替换掉的工作
    if(!this->R_device.hasReflect)return false;
    // 首先来还原内存区域
    for (int i = this->R_device.qEnd%this->memCnt; i != this->R_device.qFront%this->memCnt+1; ++i) {
        if(i >= this->memCnt)i=i%memCnt; // 这是个循环队列
        if(this->R_device.blkNums[i]!=this->blkNums[i]){
            // 如果不相等则读回此前的块
            freeBlockInBuffer(this->memBlocks[i], this->buff);
            this->memBlocks[i] = getBlockFromDiskToBuf(this->R_device.blkNums[i], this->buff);
        }
    }
    // 还原其他的信息
    this->qEnd = this->R_device.qEnd;
    this->qFront = this->R_device.qFront;
    memcpy(this->blkNums, this->R_device.blkNums, sizeof(int)*memCnt);
    this->qLength = this->R_device.qLength;
    this->block = this->R_device.block;
    this->tuple = this->R_device.tuple;
    return true;
}

void projection(int rel, int row){
    // 投影函数，输入待投影的关系和列号
    // TODO: 完成读取迭代器，完成这个功能
    return;
}

void printIO(Buffer *buff){
    // 打印IO次数
    std::cout << '\n' << "IO's is " << buff->numIO << std::endl;
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

    // 选择测试

//    selectFromRel_linear(40, RELATION_R, 1000);
//
//    selectFromRel_linear(60, RELATION_S, 1100);

    std::cout<<"Empty Blocks: "<<buf.numFreeBlk<<std::endl;

    sortRel(RELATION_R, 1200);

    auto readIter = new readBlocks(1200, 1215, 1, &buf);

    for (int j = 0; j < 25; ++j) {
        std::cout<<readIter->getVal(0)<<std::endl;
    }

//    selectFromRel_Binary(40, 1200, 1215, 1400);

//    sortRel(RELATION_S, 1500);

//    printIO(&buf);
//    selectFromRel_Binary(60, 1500, 1531, 1700);
//    printIO(&buf);

    printIO(&buf);
    return 0;
}
