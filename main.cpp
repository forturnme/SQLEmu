#include <iostream>

// MEM maneuvering
#include "util.h"
#include "readBlocks.h"
#include "writeBlock.h"

// B+ Tree implementing
#include "predefined.h"
#include "bpt.h"
#include "bpt.cc"

#define RELATION_R 0
#define RELATION_S 1

#define REL_START_END(src, firstBlkNum, lastBlkNum) switch(src){case 0:firstBlkNum = 1;lastBlkNum = 16;break;case 1:firstBlkNum = 20;lastBlkNum = 51;break;default:return;}
#define REL_FIRST_VALUE_VALID(src, value) switch(src){case 0:if(value<1||value>40)return;break;case 1:if(value<20||value>60)return;break;default:return;}


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

void projection(int rel, int row, int blkStartNum){
    // 投影函数，输入待投影的关系和列号、结果的起始编号
    int firstBlkNum = 0, lastBlkNum = 0;
    REL_START_END(rel,firstBlkNum,lastBlkNum)
    auto readBlk = new readBlocks(firstBlkNum, lastBlkNum, 7, &buf);
    auto writeBlk = new writeBufferBlock(&buf, blkStartNum);
    char writeLn[9] = {0};
    int counter = 0;
    // 开始投影一列
    for (int i = readBlk->getVal(row); i != -1 ; i = readBlk->getVal(row)) {
        if(counter%2==0&&counter!=0){
            // 满两个写一次
            writeBlk->writeOneTuple((unsigned char*)writeLn);
            bzero(writeLn, 9* sizeof(char));
        }
        itoa(i, counter%2==1?writeLn+4:writeLn, 10);
        counter++;
    }
    if(counter)writeBlk->writeOneTuple((unsigned char*)writeLn); // 需要再加一次
    delete(readBlk);
    delete(writeBlk);
}

void destinct(int relStart ,int relEnd, int blkStartNum){
    // 对排序好的块进行去重
    // TODO: 使用readBlocks完成去重
    char dbuf[9] = {0}; // 当前扫视的块放这里
}

void nestLoopJoin(int blkStartNum){
    // 简单的连接算法
    auto writeBlk = new writeBufferBlock(&buf, blkStartNum);
    char wbuf[13] = {0};// 写入缓存
    unsigned char* rblk, *sblk; // R块的地址
    int rv0; // R的首个值
    int sv0; // S的首个值
    for(int i=1;i<17;i++){
        // 依次装入r的块
        rblk = getBlockFromDiskToBuf(i, &buf);
        auto readBlks_S = new readBlocks(20, 51, 6, &buf);
        while(true){
            sv0 = readBlks_S->getValSilent(0);
            sblk = readBlks_S->getTuple();
            if(sblk == NULL)break;
            bzero(wbuf, 13*sizeof(char));
            memcpy(wbuf, sblk, 4* sizeof(unsigned char));
            memcpy(wbuf+8, sblk+4, 4* sizeof(unsigned char));
            // 对每个S值在已经读进来的R找对应关系
            for (int j = 0; j < 7; ++j) {
                rv0 = getNthTupleY(rblk, j, 0);
                if(rv0 <= 0) break;
                if(rv0==sv0){
                    memcpy(wbuf+4, rblk+j*8+4, 4* sizeof(char));
                    writeBlk->writeOneLongTuple((unsigned char*)wbuf);
                }
            }
        }
        freeBlockInBuffer(rblk, &buf);
        delete(readBlks_S);
    }
    delete(writeBlk);
}

void sortMergeJoin(int startBlk){
    // 排序链接
    sortRel(RELATION_R, 10000);
    sortRel(RELATION_S, 10020);
    auto readBlkR = new readBlocks(10000, 10015, 1, &buf);
    auto readBlkS = new readBlocks(10020, 10051, 6, &buf);
    auto writeBlk = new writeBufferBlock(&buf, startBlk);
    char wbuf[13] = {0};// 写入缓存
    int probe = 0; // 目前扫视的值
    int sval;
    unsigned char* rblk;
    // 以R为外，S为内，对R的每个值进行连接
    while (true){
        if(readBlkR->getValSilent(0)!=probe){
            // 扫描到新的R值，则更新探针以及刷新S的内存，将此位置记录
            probe = readBlkR->getValSilent(0);
            readBlkS->refresh(); // 将6块更新为未扫描的
            readBlkS->doSnapshot();
        } else readBlkS->recall(); // 回到之前更新探针的值
        // 获得新的R元组
        rblk = readBlkR->getTuple(); // 此时R已经前进了
        if(rblk==NULL){
            break;
        }
        bzero(wbuf, 13* sizeof(char));
        memcpy(wbuf, rblk, 8* sizeof(unsigned char));
        while (true) {
            // 开始检查S中的元组，直到第一个大于探针的值出现为止
            sval = readBlkS->getValSilent(0);
            if(sval > probe)break;
            if(sval == probe){
                memcpy(wbuf+8, readBlkS->getTupleSilent()+4, 4* sizeof(char));
                writeBlk->writeOneLongTuple((unsigned char*)wbuf);
            }
            if(readBlkS->getTuple()==NULL){
                break;
            } // 此时S已经前进了
        }
    }
    delete(readBlkR);
    delete(readBlkS);
    delete(writeBlk);
}

int main() {
    // 以例程为脚手架
    unsigned char *blk; /* A pointer to a block */

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

//    nestLoopJoin(1800);
    sortMergeJoin(1900);
//
//    projection(RELATION_S, 0, 1700);

//    sortRel(RELATION_R, 1200);
//    sortRel(RELATION_S, 1400);

//    auto readIter = new readBlocks(1400, 1431, 6, &buf);
//    std::cout<<"Empty Blocks: "<<buf.numFreeBlk<<std::endl;
//    readIter->refresh();
//    std::cout<<"Empty Blocks: "<<buf.numFreeBlk<<std::endl;
//

//    for (int j = 0; j < 24; ++j) {
//        if(j==3) readIter->doSnapshot();
//        if(j==15) readIter->recall();
//        std::cout<<readIter->getVal(0)<<std::endl;
//    }
//
//    std::cout<<"Empty Blocks: "<<buf.numFreeBlk<<std::endl;
//    readIter->refresh();
//    delete readIter;
//    std::cout<<"Empty Blocks: "<<buf.numFreeBlk<<std::endl;

//    selectFromRel_Binary(40, 1200, 1215, 1400);

//    sortRel(RELATION_S, 1500);
//
//    printIO(&buf);
//    selectFromRel_Binary(60, 1500, 1531, 1700);
//    printIO(&buf);

    printIO(&buf);
    return 0;
}
