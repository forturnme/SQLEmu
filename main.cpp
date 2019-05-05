/*
 * Created by longh in 2019
 */

#include <iostream>
#include <cstdlib>

// MEM maneuvering
#include "util.h"
#include "readBlocks.h"
#include "writeBlock.h"

// B+ Tree implementing
#include "bpt_disx.h"

#define RELATION_R 0
#define RELATION_S 1

// 有序区
#define SORTED_R 8000
#define SORTED_S 8020
// B+树的存放位置
#define BPT_R 9000
#define BPT_S 9020

#define REL_START_END(src, firstBlkNum, lastBlkNum) \
    switch(src){case 0:firstBlkNum = 1;lastBlkNum = 16; \
    break;case 1:firstBlkNum = 20;lastBlkNum = 51;break;default:return;}
#define REL_FIRST_VALUE_VALID(src, value) \
    switch(src){case 0:if(value<1||value>40)return;break; \
    case 1:if(value<20||value>60)return;break;default:return;}

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
        auto *writeBlk = new writeBufferBlock(&buf, startBlock+2000+i*7);
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
        blk[i] = getBlockFromDiskToBuf(startBlock + 2000 + i * 7, &buf);
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
                blk[i] = getBlockFromDiskToBuf(changed[i]+i*7+startBlock+2000, &buf);
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
            sblk = readBlks_S->getTupleSilent();
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
            // 若无法前进则退出
            if(!readBlks_S->forward())break;
        }
        freeBlockInBuffer(rblk, &buf);
        delete(readBlks_S);
    }
    delete(writeBlk);
}

void sortMergeJoin(int startBlk, bool sorted){
    // 排序链接
    if(!sorted){
        sortRel(RELATION_R, SORTED_R);
        sortRel(RELATION_S, SORTED_S);
    }
    auto readBlkR = new readBlocks(SORTED_R, SORTED_R+15, 1, &buf);
    auto readBlkS = new readBlocks(SORTED_S, SORTED_S+31, 6, &buf);
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
        rblk = readBlkR->getTupleSilent();
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
        // 前移R，若没有下一条目则结束
        if(readBlkR->end())break;
        readBlkR->forward();
    }
    delete(readBlkR);
    delete(readBlkS);
    delete(writeBlk);
}

void doIntersection(int startBlk, bool sorted){
    // 求交，从Sort-Merge-Join改过来的
    if(!sorted){
        sortRel(RELATION_R, SORTED_R);
        sortRel(RELATION_S, SORTED_S);
    }
    auto readBlkR = new readBlocks(SORTED_R, 15+SORTED_R, 1, &buf);
    auto readBlkS = new readBlocks(SORTED_S, 31+SORTED_S, 6, &buf);
    auto writeBlk = new writeBufferBlock(&buf, startBlk);
    int probe = 0; // 目前扫视的值
    int sval, rval1;
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
        rval1 = readBlkR->getValSilent(1);
        rblk = readBlkR->getTuple(); // 此时R已经前进了
        if(rblk==NULL){
            break;
        }
        while (true) {
            // 开始检查S中的元组，直到第一个大于探针的值出现为止
            sval = readBlkS->getValSilent(0);
            if(sval > probe)break;
            if(sval==probe&&rval1==readBlkS->getValSilent(1)){
                // 如果有一样的就写
                writeBlk->writeOneTuple(readBlkS->getTupleSilent());
                break;
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

void doDiff(int startBlk, int mode, bool sorted){
    // 求差，从求交改过来的
    // mode为0表示R-S，为1表示S-R
    if(!sorted){
        // 没排过序就排一次
        sortRel(RELATION_R, SORTED_R);
        sortRel(RELATION_S, SORTED_S);
    }
    readBlocks* readBlkR;
    readBlocks* readBlkS;
    if(mode==0){
        readBlkR = new readBlocks(SORTED_R, SORTED_R+15, 1, &buf);
        readBlkS = new readBlocks(SORTED_S, SORTED_S+31, 6, &buf);
    }else{
        readBlkR = new readBlocks(SORTED_S, SORTED_S+31, 1, &buf);
        readBlkS = new readBlocks(SORTED_R, SORTED_R+15, 6, &buf);
    }
    auto writeBlk = new writeBufferBlock(&buf, startBlk);
    int probe = 0; // 目前扫视的值
    int sval, rval1;
    unsigned char* rblk;
    bool same;
    // 以R为外，S为内，对R的每个值进行连接
    while (true){
        if(readBlkR->getValSilent(0)!=probe){
            // 扫描到新的R值，则更新探针以及刷新S的内存，将此位置记录
            probe = readBlkR->getValSilent(0);
            readBlkS->refresh(); // 将6块更新为未扫描的
            readBlkS->doSnapshot();
        } else readBlkS->recall(); // 回到之前更新探针的值
        // 获得新的R元组
        rval1 = readBlkR->getValSilent(1);
        rblk = readBlkR->getTupleSilent();
        same = false;
        while (true) {
            // 开始检查S中的元组，直到第一个大于探针的值出现为止
            sval = readBlkS->getValSilent(0);
            if(sval > probe)break;
            if(sval == probe&&rval1==readBlkS->getValSilent(1)){
                // 如果没有一样的就写
                same = true;
                break;
            }
            if(readBlkS->end())break;
            readBlkS->forward();
        }
        if(!same)writeBlk->writeOneTuple(rblk);
        if(readBlkR->end())break;
        readBlkR->forward();
    }
    delete(readBlkR);
    delete(readBlkS);
    delete(writeBlk);
}

void doUnion(int startBlk, bool sorted){
    // 交运算，实际上此步偷懒了，直接两个差加一个并，毫无复用性^_^
    if(!sorted){
        // 没排过序就排一次
        sortRel(RELATION_R, SORTED_R);
        sortRel(RELATION_S, SORTED_S);
    }
    readBlocks* readBlkR;
    readBlocks* readBlkS;
    auto writeBlk = new writeBufferBlock(&buf, startBlk);
    int probe; // 目前扫视的值
    int sval, rval1;
    unsigned char* rblk;
    bool same;
    // 做两次差，屑操作
    for(int i = 0; i < 2; i++){
        if(i==0){
            readBlkR = new readBlocks(SORTED_R, SORTED_R+15, 1, &buf);
            readBlkS = new readBlocks(SORTED_S, SORTED_S+31, 6, &buf);
        } else{
            readBlkR = new readBlocks(SORTED_S, SORTED_S+31, 1, &buf);
            readBlkS = new readBlocks(SORTED_R, SORTED_R+15, 6, &buf);
        }
        // 以R为外，S为内，对R的每个值进行连接
        probe = 0;
        while (true){
            if(readBlkR->getValSilent(0)!=probe){
                // 扫描到新的R值，则更新探针以及刷新S的内存，将此位置记录
                probe = readBlkR->getValSilent(0);
                readBlkS->refresh(); // 将6块更新为未扫描的
                readBlkS->doSnapshot();
            } else readBlkS->recall(); // 回到之前更新探针的值
            // 获得新的R元组
            rval1 = readBlkR->getValSilent(1);
            rblk = readBlkR->getTupleSilent(); // 此时R已经前进了
            same = false;
            while (true) {
                // 开始检查S中的元组，直到第一个大于探针的值出现为止
                sval = readBlkS->getValSilent(0);
                if(sval > probe)break;
                if(sval == probe&&rval1==readBlkS->getValSilent(1)){
                    // 如果没有一样的就写
                    same = true;
                    break;
                }
                if(readBlkS->getTuple()==NULL){
                    break;
                } // 此时S已经前进了
            }
            if(!same)writeBlk->writeOneTuple(rblk);
            if(readBlkR->end())break;
            readBlkR->forward();
        }
        delete(readBlkR);
        delete(readBlkS);
    }
    // 做一次交，屑操作再临
    probe = 0;
    readBlkR = new readBlocks(SORTED_R, SORTED_R+15, 1, &buf);
    readBlkS = new readBlocks(SORTED_S, SORTED_S+31, 6, &buf);
    while (true){
        if(readBlkR->getValSilent(0)!=probe){
            // 扫描到新的R值，则更新探针以及刷新S的内存，将此位置记录
            probe = readBlkR->getValSilent(0);
            readBlkS->refresh(); // 将6块更新为未扫描的
            readBlkS->doSnapshot();
        } else readBlkS->recall(); // 回到之前更新探针的值
        // 获得新的R元组
        rval1 = readBlkR->getValSilent(1);
        rblk = readBlkR->getTuple(); // 此时R已经前进了
        if(rblk==NULL){
            break;
        }
        while (true) {
            // 开始检查S中的元组，直到第一个大于探针的值出现为止
            sval = readBlkS->getValSilent(0);
            if(sval > probe)break;
            if(sval==probe&&rval1==readBlkS->getValSilent(1)){
                // 如果有一样的就写
                writeBlk->writeOneTuple(rblk);
                break;
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

void nestLoopHashJoin(int startBlk){
    // 由于内存区域过少，故无法实现纯hash join
    // 第一步：先设置6个写缓存区域作为哈希桶，剩下的一个作为输入缓存，另一个留作输出缓存
    // 先读小表，做哈希
    auto read = new readBlocks(1, 16, 1, &buf);
    // 申请6个写缓存
    writeBufferBlock* write[6];
    for (int i = 0; i < 6; ++i) {
        write[i] = new writeBufferBlock(&buf, i*10+startBlk+20000);
    }
    while (true){
        // 逐个做哈希
        write[read->getValSilent(0)%6]->writeOneTuple(read->getTupleSilent());
        if(read->end())break;
        read->forward();
    }
    // 记录有多少哈希块被生成了，之后删除写缓存
    int hashBlkCnts[6];
    for (int i = 0; i < 6; ++i) {
        hashBlkCnts[i] = write[i]->writtenBlksExpected();
        delete(write[i]);
    }
    delete(read);
    auto joinBuf = new writeBufferBlock(&buf, startBlk); // 最终结果的写缓存
    read = new readBlocks(20, 51, 1, &buf); // 读来S表
    readBlocks* hashes[6]; // 装哈希表
    for (int i = 0; i < 6; ++i) {
        if(hashBlkCnts[i]>0){
            hashes[i] = new readBlocks(i*10+startBlk+20000, i*10+startBlk+20000+hashBlkCnts[i]-1, 1, &buf);
            hashes[i]->doSnapshot(); // 供回溯用
        }
        else hashes[i] = NULL;
    }
    int sval0 = 0, ind=0; // 暂存主键值
    char bar[13] = {0}; // 用于构造写进元组
    while (true){
        // 读上来S的元组做哈希
        sval0 = read->getValSilent(0);
        ind = sval0%6;
        if(hashes[ind]){
            // 准备写入的三元组
            bzero(bar, 13*sizeof(char));
            memcpy(bar, read->getTupleSilent(), 4*sizeof(char));
            memcpy(bar+8, read->getTupleSilent()+4, 4*sizeof(char));
            // 对哈希表全部元组做连接测试
            while (true){
                if(hashes[ind]->getValSilent(0)==sval0){
                    // 命中，写入
                    memcpy(bar+4, hashes[ind]->getTupleSilent()+4, 4*sizeof(char));
                    joinBuf->writeOneLongTuple((unsigned char*)bar);
                }
                if(hashes[ind]->end())break;
                hashes[ind]->forward();
            }
            hashes[ind]->recall();
        }
        if(read->end())break;
        read->forward();
    }
    // 清理内存
    for (int i = 0; i < 6; ++i) {
        delete(hashes[i]);
    }
    delete(joinBuf);
}

void hashNestLoopJoin(int startBlk){
    // 由于内存区域过少，故无法实现纯hash join
    // 第一步：先设置6个写缓存区域作为哈希桶，剩下的一个作为输入缓存，另一个留作输出缓存
    // 先读两张表，都做哈希
    auto read = new readBlocks(1, 16, 1, &buf);
    // 申请6个写缓存
    writeBufferBlock* write[6];
    for (int i = 0; i < 6; ++i) {
        write[i] = new writeBufferBlock(&buf, i*20+startBlk+20000);
    }
    while (true){
        // 逐个做哈希
        write[read->getValSilent(0)%6]->writeOneTuple(read->getTupleSilent());
        if(read->end())break;
        read->forward();
    }
    // 记录有多少哈希块被生成了，之后删除写缓存
    int hashBlkCntsR[6];
    for (int i = 0; i < 6; ++i) {
        hashBlkCntsR[i] = write[i]->writtenBlksExpected();
        delete(write[i]);
    }
    delete(read);
    // 把s也全部读入，做哈希
    read = new readBlocks(20, 51, 1, &buf);
    for (int i = 0; i < 6; ++i) {
        write[i] = new writeBufferBlock(&buf, i*20+startBlk+200+20000);
    }
    while (true){
        // 逐个做哈希
        write[read->getValSilent(0)%6]->writeOneTuple(read->getTupleSilent());
        if(read->end())break;
        read->forward();
    }
    // 记录有多少哈希块被生成了，之后删除写缓存
    int hashBlkCntsS[6];
    for (int i = 0; i < 6; ++i) {
        hashBlkCntsS[i] = write[i]->writtenBlksExpected();
        delete(write[i]);
    }
    delete(read);
    int rval0 = 0, ind = 0; // 暂存主键值
    char bar[13] = {0}; // 用于构造写进元组
    auto joinBuf = new writeBufferBlock(&buf, startBlk); // 最终结果的写缓存
    readBlocks* readR, *readS; // 两个哈希表的读缓存，分别占1、6块
    for (int i = 0; i < 6; ++i) {
        readR = new readBlocks(i*20+startBlk+20000, i*20+startBlk+20000+hashBlkCntsR[i]-1, 1, &buf);
        readS = new readBlocks(i*20+startBlk+200+20000, i*20+startBlk+200+20000+hashBlkCntsS[i]-1, 6, &buf);
        readS->refresh();
        readS->doSnapshot();
        while (true){
            // 用R里面每条连S
            rval0 = readR->getValSilent(0);
            // 先构造输出缓存
            bzero(bar, 13*sizeof(char));
            memcpy(bar, readR->getTupleSilent(), 8*sizeof(char));
            readS->recall();
            while (true){
                // 遍历内存里的S元组
                if(readS->getValSilent(0)==rval0){
                    memcpy(bar+4, readS->getTupleSilent()+4, 4* sizeof(char));
                    joinBuf->writeOneLongTuple((unsigned char*)bar);
                }
                if(readS->end())break;
                readS->forward();
            }
            if(readR->end())break;
            readR->forward();
        }
        delete(readR);
        delete(readS);
    }
    delete(joinBuf);
}

void showTabHead(int lineCnt, int rel){
    // 显示表头
    if(lineCnt == 1) std::cout<<"INDEX | VALUE"<<std::endl;
    if(lineCnt == 2 && rel == RELATION_R) std::cout<<"INDEX | R.A     R.B"<<std::endl;
    if(lineCnt == 2 && rel == 2) std::cout<<"INDEX | P1      P2"<<std::endl;
    if(lineCnt == 2 && rel == RELATION_S) std::cout<<"INDEX | S.C     S.D"<<std::endl;
    if(lineCnt == 3) std::cout<<"INDEX | A       B       C"<<std::endl;
}

void showBlocksInDisc(int start){
    // 显示磁盘上从start开始的块
    std::cout<<"________________________"<<std::endl;
    auto read = new readBlocks(start, 99999, 7, &buf);
    char bar[5] = {0};
    int row = 0;
    for(unsigned char* i=read->getTuple();i!=NULL;i=read->getTuple()){
        row++;
        bzero(bar, 5*sizeof(char));
        memcpy(bar, i, 4* sizeof(char));
        printf("%5d | %s\t", row, bar);
        bzero(bar, 5*sizeof(char));
        memcpy(bar, i+4, 4* sizeof(char));
        printf("%s\n", bar);
    }
    printf("\n< %d Rows x 2 Cols >\n\n", row);
    delete(read);
}

void showBlocksInDiscLong(int start){
    // 显示磁盘上从start开始的三元块
    int row = 0, flg = 0;
    std::cout<<"________________________________"<<std::endl;
    for (int i = start;; ++i) {
        auto read = new readBlocks(i, i, 1, &buf);
        char bar[5] = {0};
        unsigned char* p=read->getTupleSilent();
        for(int j=0;j<4;j++){
            bzero(bar, 5*sizeof(char));
            memcpy(bar, p+j*12, 4* sizeof(char));
            if(atoi(bar)==0)break;
            row++;
            printf("%5d | %s\t", row, bar);
            bzero(bar, 5*sizeof(char));
            memcpy(bar, p+4+j*12, 4* sizeof(char));
            printf("%s\t", bar);
            bzero(bar, 5*sizeof(char));
            memcpy(bar, p+8+j*12, 4* sizeof(char));
            printf("%s\n", bar);
        }
        // 获取续地址
        flg = fourCharToInt(p+56);
        delete(read);
        if(flg==0)break;
    }
    printf("\n< %d Rows x 3 Cols >\n\n", row);
}

void showBlocksInDiscShort(int start){
    // 显示磁盘上从start开始的单元块
    int row = 0, flg = 0;
    std::cout<<"________________"<<std::endl;
    for (int i = start;; ++i) {
        auto read = new readBlocks(i, i, 1, &buf);
        char bar[5] = {0};
        unsigned char* p=read->getTupleSilent();
        for(int j=0;j<14;j++){
            bzero(bar, 5*sizeof(char));
            memcpy(bar, p+j*4, 4* sizeof(char));
            if(atoi(bar)==0)break;
            row++;
            printf("%5d | %s\t\n", row, bar);
        }
        // 获取续地址
        flg = fourCharToInt(p+56);
        delete(read);
        if(flg==0)break;
    }
    printf("\n< %d Rows x 1 Cols >\n\n", row);
}

void showMainMenu(){
    // 显示Logo和主菜单
    // 这是Logo（笑）
    std::cout<<R"(%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%)"<<std::endl;
    std::cout<<R"( _______   _______   _          _______   _______           )"<<std::endl;
    std::cout<<R"((  ____ \ (  ___  ) ( \        (  ____ \ (       ) |\     /|)"<<std::endl;
    std::cout<<R"(| (    \/ | (   ) | | (        | (    \/ | () () | | )   ( |)"<<std::endl;
    std::cout<<R"(| (_____  | |   | | | |        | (__     | || || | | |   | |)"<<std::endl;
    std::cout<<R"((_____  ) | |   | | | |        |  __)    | |(_)| | | |   | |)"<<std::endl;
    std::cout<<R"(      ) | | | /\| | | |        | (       | |   | | | |   | |)"<<std::endl;
    std::cout<<R"(/\____) | | (_\ \ | | (____/\  | (____/\ | )   ( | | (___) |)"<<std::endl;
    std::cout<<R"(\_______) (____\/_) (_______/  (_______/ |/     \| (_______))"<<std::endl;
    std::cout                                                                   <<std::endl;
    std::cout<<R"(%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%)"<<std::endl;
    std::cout<<R"(       (C) longh 2019. Copy Left. No Rights Reserved.       )"<<std::endl;
    std::cout                                                                   <<std::endl;

    // 这是主菜单
    std::cout<<R"(################# PLEASE ENTER YOUR CHOICE #################)"<<std::endl;
    std::cout<<R"(##           1.Choose tuples from relationship            ##)"<<std::endl;
    std::cout<<R"(##           2.Do projection with relationship            ##)"<<std::endl;
    std::cout<<R"(##           3.Join relationships and save                ##)"<<std::endl;
    std::cout<<R"(##           4.Union, Intersection or Differentation      ##)"<<std::endl;
    std::cout<<R"(##           0.(or any other thing) Quit program          ##)"<<std::endl;
    std::cout<<R"(############################################################)"<<std::endl;
    std::cout                                                                   <<std::endl;
    std::cout<<R"(Your choice: )";
}

void chooseTupleMenu(BPT_Disx* bptr, BPT_Disx* bpts){
    // 选择元组的菜单
    // 两个B+树不是全局变量，需要传进来...
    char sel = 0;
    while (sel != 'r' && sel != 's'){
        fflush(stdin);
        std::cout<<R"(Which relationship do you want to select? (r/s) )";
        std::cin>>sel;
    }
    char method = 0;
    while (method != 'L' && method != 'B' && method != 'T'){
        fflush(stdin);
        std::cout<<R"(Which method do you want to select? (L for linear, B for binary, T for b+tree) )";
        std::cin>>method;
    }
    int val = 0;
    fflush(stdin);
    std::cout<<R"(Enter a value you want to search: )";
    std::cin>>val;
    int blk = 0; // 开始存储的块号
    while (blk < 100 || blk >= 1000){
        fflush(stdin);
        std::cout<<R"(Enter start block less than 1000 but more than 100 that you want to store result: )";
        std::cin>>blk;
    }
    int IO_old = buf.numIO;
    int rel = sel=='r'?RELATION_R:RELATION_S; // 指示关系
    switch (method){
        case 'L':
            selectFromRel_linear(val, rel, blk);
            break;
        case 'B':
            selectFromRel_Binary(val, rel==0?SORTED_R:SORTED_S, rel==0?SORTED_R+15:SORTED_S+31, blk);
            break;
        case 'T':
            if(rel==0){
                bptr->find(val, blk);
            } else{
                bpts->find(val, blk);
            }
            break;
        default:
            break;
    }
    //  显示结果
    std::cout<<"You queried for val "<<val<<std::endl;
    IO_old = buf.numIO-IO_old;
    showTabHead(2, rel);
    showBlocksInDisc(blk);
    std::cout<<"[IO's for this query] "<<IO_old<<std::endl;
    system("pause");
}

void projectionMenu(){
    // 做投影的菜单
    char sel = 0;
    while (sel != 'r' && sel != 's'){
        fflush(stdin);
        std::cout<<R"(Which relationship do you want to select? (r/s) )";
        std::cin>>sel;
    }
    int row = -1;
    while (row != 0 && row != 1){
        fflush(stdin);
        std::cout<<R"(Which row do you want to do projection? (0/1) )";
        std::cin>>row;
    }
    int blk = 0; // 开始存储的块号
    while (blk < 100 || blk >= 1000){
        fflush(stdin);
        std::cout<<R"(Enter start block less than 1000 but more than 100 that you want to store result: )";
        std::cin>>blk;
    }
    int IO_old = buf.numIO;
    projection(sel=='r'?0:1, row, blk);
    IO_old = buf.numIO-IO_old;
    showTabHead(1, 0);
    showBlocksInDiscShort(blk);
    std::cout<<"[IO's for this query] "<<IO_old<<std::endl;
    system("pause");
}

void joinMenu(){
    // 做连接的菜单
    char method = 0;
    while (method != 'h' && method != 'H' && method != 'n' && method != 's'){
        fflush(stdin);
        std::cout<<R"(Which join method do you want to use? (H for nest loop hash join, h for hash nest loop join, n for nest loop join, s for sort merge join) )";
        std::cin>>method;
    }
    int blk = 0; // 开始存储的块号
    while (blk < 100 || blk >= 1000){
        fflush(stdin);
        std::cout<<R"(Enter start block less than 1000 but more than 100 that you want to store result: )";
        std::cin>>blk;
    }
    int IO_old = buf.numIO;
    switch (method){
        case 'H':
            nestLoopHashJoin(blk);
            break;
        case 'h':
            hashNestLoopJoin(blk);
            break;
        case 's':
            sortMergeJoin(blk, true);
            break;
        case 'n':
            nestLoopJoin(blk);
            break;
        default:
            break;
    }
    IO_old = buf.numIO-IO_old;
    showTabHead(3, 0);
    showBlocksInDiscLong(blk);
    std::cout<<"[IO's for this query] "<<IO_old<<std::endl;
    system("pause");
}

void setAlgorithmMenu(){
    // 集合代数相关功能
    char method = 0;
    while (method != 'u' && method != 'd' && method != 'i'){
        fflush(stdin);
        std::cout<<R"(Which set operation do you want to use? (u for Union, d for Differentation, i for Intersection) )";
        std::cin>>method;
    }
    int diffMode = -1;
    if(method == 'd'){
        while (diffMode != 0 && diffMode != 1){
            fflush(stdin);
            std::cout<<R"(Enter 0 for R-S or 1 for S-R: )";
            std::cin>>diffMode;
        }
    }
    int blk = 0; // 开始存储的块号
    while (blk < 100 || blk >= 1000){
        fflush(stdin);
        std::cout<<R"(Enter start block less than 1000 but more than 100 that you want to store result: )";
        std::cin>>blk;
    }
    int IO_old = buf.numIO;
    switch (method){
        case 'u':
            doUnion(blk, true);
            break;
        case 'd':
            doDiff(blk, diffMode, true);
            break;
        case 'i':
            doIntersection(blk, true);
            break;
        default:
            break;
    }
    IO_old = buf.numIO-IO_old;
    showTabHead(2, 2);
    showBlocksInDisc(blk);
    std::cout<<"[IO's for this query] "<<IO_old<<std::endl;
    system("pause");
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

    int IOlast = buf.numIO;
    std::cout<<"< Presorting R and S...... >"<<std::endl;
    sortRel(RELATION_R, SORTED_R);
    sortRel(RELATION_S, SORTED_S);
    std::cout<<"< Presorting completed with "<<buf.numIO-IOlast<<" IO's >"<<std::endl;

    IOlast = buf.numIO;
    std::cout<<"< Building B+ Tree...... >"<<std::endl;
    auto bptr = new BPT_Disx(SORTED_R, SORTED_R+15, BPT_R, &buf);
    auto bpts = new BPT_Disx(SORTED_S, SORTED_S+31, BPT_R, &buf);
    std::cout<<"< B+ tree built with "<<buf.numIO-IOlast<<" IO's >"<<std::endl;
    std::cout<<"< Preparation completed with "<<buf.numIO<<" IO's >"<<std::endl;


    while (true){
        showMainMenu();

        fflush(stdin);
        char c = 0;
        std::cin>>c;

        switch (c){
            case '1':
                chooseTupleMenu(bptr, bpts);
                break;
            case '2':
                projectionMenu();
                break;
            case '3':
                joinMenu();
                break;
            case '4':
                setAlgorithmMenu();
                break;
            default:
                system("./cleanblockfiles.bat");
                printIO(&buf);
                exit(0);
        }
        system("cls");
    }
}
