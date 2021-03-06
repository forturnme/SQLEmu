//
// Created by longh on 2019/5/2.
//
#ifndef SQLEMU_READBLOCKS_H
#define SQLEMU_READBLOCKS_H

#include "util.h"

class readBlocks{
    /* 读取内存块，专门用来读元组的。
     * 可以指定：
     *         -这个区域占有内存块的数量
     *         -能读取的块号范围
     *         -读到0地址结尾的块后也会停止
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
    bool forward(); // 指针移动一个单位，若无法移动则无事发生并返回false
    unsigned char* getTuple(); // 得到元组地址，指针移动，若无法移动则不移动
    int getValSilent(int which); // 得到元组中的第which个元素
    int getVal(int which); // 得到之后还移动
    bool doSnapshot(); // 存储当前的状态
    bool recall(); // 还原此前的状态
    bool refresh(); // 更换指针所在块前面的块
    bool end(); // 判断读取是否到达末端
    int getCurrentBlkNum(); // 返回当前指针在的块号(*****.blk中的*****)
private:
    bool finish= false; // 是否读到最后一项
    int startBlock=0; // 开始的磁盘块编号
    int endBlock=0; // 最后一个磁盘块的编号
    int qFront=0; // 下面两个循环队列的队首号
    int qEnd=0; // 队尾号
    bool qForward(); // 队列向前生长
    inline int qIndex(int n){ // 返回第n项对应的真实下标
        return (qEnd+n)%(memCnt+1);
    }
    inline void qShrink(); // 队列向前收缩
    inline bool isFront(int n); // 判断n是不是在队列头
    inline unsigned char*& getNthBlock(int n); // 获得队列中第n个地址
    inline int& getNthNumber(int n); // 获取队列中第n个块号
    inline int getNthNumberVal(int n); // 获取队列中第n个块号
    inline bool toNextBlock(); // 使指针移到下个块，不能移动则返回false
    inline int qLength(){// 返回队列长度
        return (qFront-qEnd+memCnt+1)%(memCnt+1);
    }
    unsigned char** memBlocks=NULL; // 持有的内存块地址列表，是循环队列
    int* blkNums=NULL; // 对应的磁盘块编号，是循环队列
    int block=0; // 现在扫描到了何块
    int tuple=0; // 现在扫描到了哪个元组
    struct {
        bool finish=false;
        bool hasReflect= false; // 有没有使用R装置储存事象
        int qFront=0; // 下面两个循环队列的队首号，等于新元素将插入的位置
        int qEnd=0; // 队尾号
        int* blkNums=NULL; // 对应的磁盘块编号
        int block=0; // 现在扫描到了何块
        int tuple=0; // 现在扫描到了哪个元组
    }R_device; // 存储事象供回溯使用
    inline bool full(); // 判断队列是否满
    inline bool empty(); // 判断队列是否空
    bool loadBlkFromDisc(int blkNum); // 装载一块进入磁盘
    bool removeLastBlk(); // 移除储存的最后一块
};

readBlocks::readBlocks(int startBlk, int endBlk, int memCnt, Buffer *buff) {
    // 生成读取区域。输入开始块编号，结束块编号，持有内存块数，对应缓冲区
    this->buff = buff;
    this->memCnt = memCnt;
    this->blkNums = (int*)malloc(sizeof(int)*(memCnt+1));
    this->memBlocks = (unsigned char**)malloc(sizeof(unsigned char*)*(memCnt+1));
    this->R_device.blkNums = (int*)malloc(sizeof(int)*(memCnt+1));
    bzero(this->memBlocks, sizeof(unsigned char*)*(memCnt+1));
    bzero(this->R_device.blkNums, sizeof(int)*(memCnt+1));
    bzero(this->blkNums, sizeof(int)*(memCnt+1));
    this->startBlock = startBlk;
    this->endBlock = endBlk;
    // 装入第一块来开张
    this->loadBlkFromDisc(this->startBlock);
}

bool readBlocks::doSnapshot() {
    // 做快照
    memcpy(this->R_device.blkNums, this->blkNums, sizeof(int)*(memCnt+1));
    this->R_device.block = this->block;
    this->R_device.finish = this->finish;
    this->R_device.qEnd = this->qEnd;
    this->R_device.qFront = this->qFront;
    this->R_device.tuple = this->tuple;
    this->R_device.hasReflect = true;
    return true;
}

inline bool readBlocks::toNextBlock() {
    // 块指针后移1个
    if(this->isFront(this->block)){
        int blkNumNow = this->getNthNumberVal(this->block);
        if(fourCharToInt(getNthBlock(block)+56)==0 || blkNumNow >= this->endBlock) return false;
        if(full())this->removeLastBlk();
        this->loadBlkFromDisc(blkNumNow+1);
    }
    this->block++;
    return true;
}

bool readBlocks::qForward() {
    // 队首前进
    if(this->full()){
        perror("ERROR 120: read blocks reached an illegal area");
        return false;
    }
    qFront = (qFront+1)%(memCnt+1);
    return true;
}

inline void readBlocks::qShrink() {
    // 收缩队列
    if(!empty()){
        this->qEnd=(qEnd+1)%(memCnt+1);
    }
}

inline int& readBlocks::getNthNumber(int n) {
    // 获得第n个块编号
    return this->blkNums[(n+this->qEnd)%(this->memCnt+1)];
}

inline int readBlocks::getNthNumberVal(int n) {
    // 获得第n个块编号
    return this->blkNums[(n+this->qEnd)%(this->memCnt+1)];
}

inline unsigned char*& readBlocks::getNthBlock(int n) {
    // 获得第n块
    return this->memBlocks[(n+this->qEnd)%(this->memCnt+1)];
}

inline bool readBlocks::full(){
    // 判断是否满
    return (qFront + 1) % (memCnt + 1) == qEnd;
}

inline bool readBlocks::empty(){
    // 判断是否空
    return qEnd==qFront;
}

bool readBlocks::loadBlkFromDisc(int blkNum) {
    // 装载第blkNum块进磁盘，不影响指针，但是不碍事
    if(full())return false;
    // 先推进队列
    // 然后装入blkNum块
    this->memBlocks[this->qFront] = getBlockFromDiskToBuf(blkNum, this->buff);
    this->blkNums[this->qFront] = blkNum;
    // 更新队首
    this->qForward();
    return true;
}

bool readBlocks::removeLastBlk() {
    // 删除队尾的一块内存，同时维护指针，保证指针不移动
    if(this->empty())return false;
    // 先删队尾
    freeBlockInBuffer(this->getNthBlock(0), this->buff);
    // 再缩队列
    this->qShrink();
    this->block--; // 维护块指针
    return true;
}

inline bool readBlocks::isFront(int n) {
    // 判断是否已经到队列头
    return this->qLength() == n+1;
}

bool readBlocks::forward() {
    // 前移指针一个元组
    // 如果到达最后则返回false
    if(this->finish)return false;
    if(this->tuple==6){
        // 若此块满，则要前进一块
        if(this->toNextBlock())this->tuple=0;
        else this->finish = true;
    }
    else{
        // 若没满则直接加
        if(getNthTupleY(this->getNthBlock(this->block), this->tuple+1, 0)!=0){
            this->tuple++;
        }
        else this->finish = true;
    }
    return true;
}

bool readBlocks::end() {
    // 如果到达最后则返回true
    if(fourCharToInt(getNthBlock(block)+56)==0||this->getNthNumberVal(this->block)==this->endBlock){
        if(this->tuple>=6)return true;
        return getNthTupleY(this->getNthBlock(this->block), this->tuple + 1, 0) == 0;
    }
    return false;
}

unsigned char* readBlocks::getTupleSilent() {
    // 获得当前元组的地址
    return (this->getNthBlock(this->block)+this->tuple*8);
}

unsigned char* readBlocks::getTuple() {
    // 获得当前元组地址并让地址前进
    unsigned char* t = this->getTupleSilent();
    return this->forward()?t:NULL;
}

int readBlocks::getValSilent(int which){
    // 获得元组的第n个值
    return getNthTupleY(getNthBlock(this->block), this->tuple, which);
}

int readBlocks::getVal(int which) {
    // 获得元组的第n个值，然后指针前进
    int res = this->getValSilent(which);
    return this->forward()?res:-1;
}

bool readBlocks::refresh() {
    // 刷新内存区域，尽可能地更换指针之前的块
    int blkToLoad;
    int lim = this->memCnt-this->qLength();
    for (int i = 0; i < lim; ++i) {
        // 首先把剩下的区域填充完
        // 如果队首的续地址已经是0则不继续装
        if(fourCharToInt(getNthBlock(qFront-1)+56)==0)break;
        blkToLoad = this->getNthNumber(this->qLength()-1)+1;
        if(blkToLoad>this->endBlock) break;
        this->loadBlkFromDisc(blkToLoad);
    }
    for (int i = 0; i < this->memCnt; ++i) {
        // 然后开始尽可能替换块
        // 如果队首的续地址已经是0则不继续装
        if(fourCharToInt(getNthBlock(qFront-1)+56)==0)break;
        if(this->block==0)break;
        blkToLoad = this->getNthNumber(this->qLength()-1)+1;
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
    int preLength = (this->R_device.qFront-this->R_device.qEnd+memCnt+1)%(memCnt+1);
    int nowLength = this->qLength();
    for (int i = 0; i < memCnt+1; ++i) {
        // 首先清除掉存档时未使用，但现在正使用的块
        if((i-this->qEnd+memCnt+1)%(memCnt+1)<nowLength&&
            (i-this->R_device.qEnd+memCnt+1)%(memCnt+1)>=preLength) {
            freeBlockInBuffer(this->memBlocks[i], this->buff);
        }
    }
    for (int i = 0; i < memCnt+1; ++i) {
        // 然后替换两回都在使用的内容不同的块
        if((i-this->qEnd+memCnt+1)%(memCnt+1)<nowLength&&
           (i-this->R_device.qEnd+memCnt+1)%(memCnt+1)<preLength&&
           this->R_device.blkNums[i]!=this->blkNums[i]) {
            freeBlockInBuffer(this->memBlocks[i], this->buff);
            this->memBlocks[i] = getBlockFromDiskToBuf(this->R_device.blkNums[i], this->buff);
        }
    }
    for (int i = 0; i < memCnt+1; ++i) {
        // 最后装回此前使用但现在未使用的块
        if((i-this->qEnd+memCnt+1)%(memCnt+1)>=nowLength&&
           (i-this->R_device.qEnd+memCnt+1)%(memCnt+1)<preLength) {
            this->memBlocks[i] = getBlockFromDiskToBuf(this->R_device.blkNums[i], this->buff);
        }
    }
    // 还原其他的信息
    this->qEnd = this->R_device.qEnd;
    this->finish = this->R_device.finish;
    this->qFront = this->R_device.qFront;
    memcpy(this->blkNums, this->R_device.blkNums, sizeof(int)*(memCnt+1));
    this->block = this->R_device.block;
    this->tuple = this->R_device.tuple;
    return true;
}

readBlocks::~readBlocks() {
    // 释放全部持有的内存块
    int lim = this->qLength();
    for (int i = 0; i < lim; ++i) {
        freeBlockInBuffer(this->getNthBlock(i), this->buff);
    }
    free(this->blkNums);
    free(this->memBlocks);
    if(this->R_device.hasReflect)free(this->R_device.blkNums);
}

int readBlocks::getCurrentBlkNum() {
    // 获得当前的块号（磁盘）
    return getNthNumberVal(block);
}

#endif //SQLEMU_READBLOCKS_H
