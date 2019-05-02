//
// Created by longh on 2019/5/2.
//

#ifndef SQLEMU_WRITEBLOCK_H
#define SQLEMU_WRITEBLOCK_H

#include "util.h"

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

#endif //SQLEMU_WRITEBLOCK_H
