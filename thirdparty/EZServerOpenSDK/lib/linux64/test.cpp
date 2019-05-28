/** \file      test.cpp
 *  \copyright HangZhou Hikvision System Technology Co.,Ltd. All Right Reserved.
 *  \brief     EZServerOpenSDK test demo
 *
 *  \note      flag "xxxxx" means it should write by user
 */

#include<stdio.h>
#include<iostream>
#include<fstream>
#include<pthread.h>
#include<vector>
#include<time.h>
#include<unistd.h>
#include<string.h>
#include<stdlib.h>
#include"ESOpenStream.h"
#include "json/reader.h"
#include "json/value.h"

#define LOG_PRINT_INFO printf
#define LOG_PRINT_ERROR printf

static int g_iFileSaveNum = 0;
std::string platformAddr = "https://open.ys7.com";
std::string appkey = "xxx";
std::string accesstoken = "xxx";
std::string subserial = "xxx";
std::string deviceSafeKey = "xxx";			//device verify code
int deviceChannelNo = 1;

int getADTSframe(unsigned char* buffer, int buf_size, unsigned char* data ,int* data_size){
	int size = 0;
 
	if(!buffer || !data || !data_size ){
		return -1;
	}
 
	while(1){
		if(buf_size  < 7 ){
			return -1;
		}
		//Sync words
		if((buffer[0] == 0xff) && ((buffer[1] & 0xf0) == 0xf0) ){
			size |= ((buffer[3] & 0x03) <<11);     //high 2 bit
			size |= buffer[4]<<3;                //middle 8 bit
			size |= ((buffer[5] & 0xe0)>>5);        //low 3bit
			break;
		}
		--buf_size;
		++buffer;
	}
 
	if(buf_size < size){
		return 1;
	}
 
	memcpy(data, buffer, size);
	*data_size = size;
 
	return 0;
}


int client_message_callback(HANDLE phandle,  int code, int eventType, void *pParas)
{
    LOG_PRINT_INFO("phandle:%d, code:%d, eventtype:%d\n", phandle, code, eventType);
    return 0;
}

int client_data_callback(HANDLE phandle,  unsigned int dataType, unsigned char *buf,  unsigned int buflen, void *pParas)
{
    LOG_PRINT_INFO("phandle:%d, datatype:%d, buflen:%d\n", phandle, dataType, buflen);
	char szFileName[64] = {0};
	sprintf(szFileName, "./test_%d.mp4", g_iFileSaveNum);
	std::ofstream file;
	file.open(szFileName, std::ios::binary|std::ios::app);
	file.write((const char*)buf, buflen);
	file.flush();
	file.close();
    return 0;
}

void testrealplay()
{
	ES_DEVICE_INFO stDevInfo;
    memset(&stDevInfo, 0, sizeof(stDevInfo));
    strcpy(stDevInfo.szDevSerial, subserial.c_str());
    std::string strToken = accesstoken;
    strcpy(stDevInfo.szSafeKey, deviceSafeKey.c_str());
    stDevInfo.iDevChannelNo = deviceChannelNo;
    
    ES_STREAM_CALLBACK streamcb;
    streamcb.on_recvmessage = client_message_callback;
    streamcb.on_recvdata = client_data_callback;
    streamcb.pUser = NULL;
    //TEST CASE1: Realplay
    HANDLE playHnadle = NULL;
	int iRes = ESOpenSDK_StartRealPlay(strToken.c_str(), stDevInfo, streamcb, playHnadle);
	if (ES_NOERROR == iRes)
	{
		sleep(2);
		ESOpenSDK_StopRealPlay(playHnadle);
		g_iFileSaveNum++;
	}
}

void testplayback()
{
	ES_DEVICE_INFO stDevInfo;
    memset(&stDevInfo, 0, sizeof(stDevInfo));
    strcpy(stDevInfo.szDevSerial, subserial.c_str());
    std::string strToken = accesstoken;
    strcpy(stDevInfo.szSafeKey, deviceSafeKey.c_str());
    stDevInfo.iDevChannelNo = deviceChannelNo;
    
    ES_STREAM_CALLBACK streamcb;
    streamcb.on_recvmessage = client_message_callback;
    streamcb.on_recvdata = client_data_callback;
    streamcb.pUser = NULL;
	//TEST CASE2: Video Search And PlayBack 
	 ES_RECORD_INFO stRecInfo;
	 memset(&stRecInfo, 0, sizeof(stRecInfo));
	 strcpy(stRecInfo.szStartTime, "2018-08-29 00:00:00");//2013-09-05 09:38:48
	 strcpy(stRecInfo.szStopTime, "2018-08-29 23:59:59");
	 stRecInfo.iRecType = 1;

	void* szRecInfo = NULL;
    int length = 0;

    int iRet = ESOpenSDK_SearchVideoRecord(strToken.c_str(), stDevInfo, stRecInfo, &szRecInfo, &length);
    if (ES_NOERROR == iRet)
    {
        LOG_PRINT_INFO("RecInfo:%s", szRecInfo);
        Json::Reader  reader;
        Json::Value   value;
        if (!reader.parse((char*)szRecInfo,value))
        {
            LOG_PRINT_ERROR("platform response bad, szInfo = %s", szRecInfo);
        }
        else
        {

            Json::Value data = value["data"];
            if (data.isArray())
            {
                for (unsigned int i = 0; i < data.size(); i++)
                {
                    std::string strBegin = data[i]["beginTime"].asString();
                    std::string strEnd = data[i]["endTime"].asString();
                    std::string strDownloadPath;
                    if (data[i]["downloadPath"].isString())
                    {
                        strDownloadPath = data[i]["downloadPath"].asString();
                    }
                    int iRecType = data[i]["recType"].asInt();
                    ES_RECORD_INFO stPlayRecInfo;
                    memset(&stPlayRecInfo, 0, sizeof(stPlayRecInfo));
                    strcpy(stPlayRecInfo.szStartTime, strBegin.c_str());
                    strcpy(stPlayRecInfo.szStopTime, strEnd.c_str());
                    strcpy(stPlayRecInfo.szDownloadPath, strDownloadPath.c_str());
                    stPlayRecInfo.iRecType = iRecType;
					HANDLE playbackHandle = NULL;
                    int iRes = ESOpenSDK_StartPlayBack(strToken.c_str(), stDevInfo, stPlayRecInfo, streamcb, playbackHandle);
                    if (ES_NOERROR == iRes)
                    {
                        sleep(5);
                        ESOpenSDK_StopPlayBack(playbackHandle);
						g_iFileSaveNum++;
                    }
                }
            }
        }
        ESOpenSDK_FreeData(szRecInfo);
    }
}

struct AacAdts
{
    size_t                nFrameSize;
    size_t                nAacDataSize;
    unsigned char         szAdtsHeader[7];
    unsigned char         _noPack_[1];
    unsigned char         szAdtsFrame[2048];
};

class AacReadFile
{
public:
    AacReadFile(){}
    ~AacReadFile()
    {
        Fini();
    }
    bool Init(std::string strAacFileName)
    {
        m_pAacFile = fopen ( strAacFileName.c_str() , "rb" );
        if (m_pAacFile==NULL)
            return false;
        
        rewind(m_pAacFile);
        return true;
    }
    void Fini()
    {
        if(m_pAacFile)
            fclose(m_pAacFile);
        m_pAacFile = 0;
    }
    
    bool GetOnePackage(AacAdts &tPackage)
    {
        // copy the file into the buffer:
        if(feof(m_pAacFile))
        {
            printf("eof \n");
            return false;
        }

        size_t result = fread(tPackage.szAdtsHeader, 1, sizeof(tPackage.szAdtsHeader), m_pAacFile);
        
        if(tPackage.szAdtsHeader[0] != 0xFF || (tPackage.szAdtsHeader[1] &0xF0) != 0xF0)
        {
            printf("Not Valid adts \n");
            return false;
        }

        tPackage.nFrameSize = ( size_t(tPackage.szAdtsHeader[3] &0x03) << 11 ) + (size_t(tPackage.szAdtsHeader[4]) << 3) + (size_t(tPackage.szAdtsHeader[5] &0xE0) >> 5 ) ;
        printf("Aac len=%ld\n", tPackage.nFrameSize);

        size_t nHeaderSize = 7;
        bool  bHasCrc = (tPackage.szAdtsHeader[1] &0x01)== 0;
        if( bHasCrc == 0)
            nHeaderSize = 9;
        tPackage.nAacDataSize = tPackage.nFrameSize -nHeaderSize;

        memcpy(tPackage.szAdtsFrame, tPackage.szAdtsHeader, 7);
        result = fread(tPackage.szAdtsFrame + 7, 1, tPackage.nFrameSize - 7, m_pAacFile);
        
        return true;
    }
    
public:
    FILE * m_pAacFile;
};


void testtalk()
{
	ES_DEVICE_INFO stDevInfo;
    memset(&stDevInfo, 0, sizeof(stDevInfo));
    strcpy(stDevInfo.szDevSerial, subserial.c_str());
    std::string strToken = accesstoken;
    strcpy(stDevInfo.szSafeKey, deviceSafeKey.c_str());
    stDevInfo.iDevChannelNo = deviceChannelNo;
    
    ES_STREAM_CALLBACK streamcb;
    streamcb.on_recvmessage = client_message_callback;
    streamcb.on_recvdata = client_data_callback;
    streamcb.pUser = NULL;
	//TEST CASE3: VoiceTalk
	HANDLE talkHandle = NULL;
	int iRet = ESOpenSDK_StartVoiceTalk(strToken.c_str(), stDevInfo, streamcb, talkHandle);
	if (ES_NOERROR == iRet)
	{
		sleep(5);
		while(1)
		{
			 	AacReadFile oAacReadFile;
				bool bRet       =       oAacReadFile.Init("./nnj_cbhg_16k.aac");
			 	LOG_PRINT_INFO("bRet:%d\n",bRet);
				if(!bRet)
				{
					break;
				}
				
	      AacAdts tPackage;
				while(oAacReadFile.GetOnePackage(tPackage))
				{
		        int ret = ESOpenSDK_SendVoiceTalk(talkHandle, tPackage.szAdtsFrame, tPackage.nFrameSize);
		        if(ret != 0)
		        {
		           break;
		        }
		        usleep(64000);
				}
	      oAacReadFile.Fini();
		}
		ESOpenSDK_StopVoiceTalk(talkHandle);
	}
}


int main(int argc, char* argv[])
{
    ESOpenSDK_Init(12, 1);
    ESOpenSDK_InitWithAppKey(appkey.c_str(), platformAddr.c_str());
	
	//testrealplay();
	//testplayback();
	testtalk();

    ESOpenSDK_Fini();
    return 0;
}

