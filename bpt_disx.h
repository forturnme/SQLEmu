//
// Created by longh on 2019/5/3.
//

#ifndef SQLEMU_BPT_DISX_H
#define SQLEMU_BPT_DISX_H

#include "util.h"
#include "writeBlock.h"
#include "readBlocks.h"

class BPT_Disx{
    // 用糟心的extmem小块实现的b+树，四阶
    // 未采用常规的构造方式，而是直接把有序表当叶节点，在上方搭建非叶节点
    // 没实现插入删除，只能拿来找。。。
    // 但是它确实是一个四阶的B+树，拥有完整的B+树结构，包括叶子节点之间的指针
    // 是稀疏的
public:
    int treeStartBlk; // 树根节点的块号（不能和叶结点放一起）
    int leafStartBlk; // 叶节点开始编号
    int leafEndBlk; // 叶节点结束编号
    Buffer* buff; // 利用的缓存
    BPT_Disx(int sortedStart, int sortedEnd, int treeStart, Buffer* buff); // 从排序好的关系或者关系的稠密索引建立BPT
    void find(int val, int startBlk); // 查找，只支持查找第一个属性
};

BPT_Disx::BPT_Disx(int sortedStart, int sortedEnd, int treeStart, Buffer* buff) {
    // 从sortedStart开始到sortedEnd结束的有序关系
    // 建立一颗从sortedEnd+1开始存储的B+树
    leafStartBlk = sortedStart;
    leafEndBlk = sortedEnd;
    int lastStart = treeStart; // 之前一次的开始块
    int currentVal = 0; // 这一次的值
    int formerVal = 0; // 上一次的值
    auto read = new readBlocks(sortedStart, sortedEnd, 1, buff); // 先读取有序关系
    auto write = new writeBufferBlock(buff, treeStart); // 非叶节点写入缓存
    char iNode[13] = {0}; // 用来组装节点
    /*
     * 非叶节点的结构像这样：
     * 4bit-指针（块号）
     * 4bit-值
     * 4bit-指针（接下来的值的块号）（这个确实会重复）
     * 块号以0为nil*/
    while (true){
        // 扫视所有的值，对每个首次出现的值构造
        currentVal = read->getValSilent(0);
        if(currentVal!=formerVal){
            // 扫描到更新的值，则开始创建值的节点
            if(formerVal!=0){
                itoa(formerVal, iNode+4, 10);
                itoa(read->getCurrentBlkNum(), iNode+8, 10);
                write->writeOneLongTuple((unsigned char*)iNode);
            }
            // 准备新值的节点
            bzero(iNode, 13*sizeof(char));
            itoa(read->getCurrentBlkNum(), iNode, 10);
            formerVal = currentVal;
        }
        if(read->end())break;
        read->forward();
    }
    delete(read);
    treeStart+=write->writtenBlksExpected();
    delete(write);
    // 开始逐级向上生成节点
    unsigned char* keyBlk; // 读取用
    while (true){
        write = new writeBufferBlock(buff, treeStart);
        if(treeStart-lastStart==1){
            // 如果上一轮只有一个节点生成，则将其作为根，结束
            treeStartBlk = lastStart;
            delete(write);
            break;
        }
        // 对从第二个开始的每一个非叶节点，把第一个key送去父节点
        for (int i = lastStart+1; i < treeStart; ++i) {
            keyBlk = getBlockFromDiskToBuf(i, buff);
            // 把第一个值送进父节点
            bzero(iNode, 13*sizeof(char));
            itoa(i-1, iNode, 10);
            itoa(i, iNode+8, 10);
            memcpy(iNode+4, keyBlk+4, 4* sizeof(char));
            write->writeOneLongTuple((unsigned char*)iNode);
            freeBlockInBuffer(keyBlk, buff);
        }
        lastStart = treeStart;
        treeStart+=write->writtenBlksExpected();
        delete(write);
    }
}

void BPT_Disx::find(int val, int startBlk) {
    // 查找val的值输到startBlk
    auto write = new writeBufferBlock(buff, startBlk);
    int blkNum = this->treeStartBlk; // 正查找的块号
    char bar[5] = {0};
    unsigned char* fBlk; // 正查找的块
    int left, nodeVal;
    while (blkNum>this->leafEndBlk){
        fBlk = getBlockFromDiskToBuf(blkNum, buff);
        left = -1;
        for (int i = 0; i < 4; ++i) {
            // 找左边和右边的数
            bzero(bar, 5*sizeof(char));
            memcpy(bar, fBlk+i*12+4, 4* sizeof(char));
            nodeVal = atoi(bar);
            if(nodeVal==0) break;
            if(nodeVal<=val) left=i;
            if(nodeVal>val) break;
        }
        bzero(bar, 5*sizeof(char));
        if(left==-1){
            // 说明在最左边
            memcpy(bar, fBlk, 4* sizeof(char));
        } else{
            // 说明在某个key的右边
            memcpy(bar, fBlk+left*12+8, 4* sizeof(char));
        }
        blkNum = atoi(bar);
        freeBlockInBuffer(fBlk, buff);
    }
    // 得到了要找到的值的起始块号，顺序查找
    auto read = new readBlocks(blkNum, leafEndBlk, 1, buff);
    while (true){
        nodeVal = read->getValSilent(0);
        if(nodeVal>val)break;
        if(nodeVal==val){
            // 命中，写入
            write->writeOneTuple(read->getTupleSilent());
        }
        if(read->end())break;
        read->forward();
    }
    delete(read);
    delete(write);
}

#endif //SQLEMU_BPT_DISX_H
