"""
    行情demo - 订阅合约行情
"""

from openctp_tts import mdapi


class CMdSpiImpl(mdapi.CThostFtdcMdSpi):
    """ 回调实现类 """

    def __init__(self, _api: mdapi.CThostFtdcMdApi):
        super().__init__()
        self._api = _api

    def OnFrontConnected(self):
        """ 前置连接成功 """
        print("行情前置连接成功")

        print("登录请求")
        req = mdapi.CThostFtdcReqUserLoginField()
        self._api.ReqUserLogin(req, 0)



    def OnRtnDepthMarketData(self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField):
        """ 深度行情 """
        print("行情回报：交易所代码：",pDepthMarketData.ExchangeID,
              "合约代码:", pDepthMarketData.InstrumentID,
              #"合约在交易所代码：",pDepthMarketData.ExchangeInstID,
              "最新价格:", pDepthMarketData.LastPrice,
              "今开盘价：",pDepthMarketData.OpenPrice,
              "昨收盘价：",pDepthMarketData.PreClosePrice,
              "今最高价：",pDepthMarketData.HighestPrice,
              "今最低价：",pDepthMarketData.LowestPrice,
              "成交量:", pDepthMarketData.Volume,
              "成交金额：",pDepthMarketData.Turnover,
              "持仓量:",pDepthMarketData.OpenInterest,
              "修改时间:", pDepthMarketData.UpdateTime,
              #"修改毫秒:", pDepthMarketData.UpdateMillisec,
              #"当日均价:", pDepthMarketData.AveragePrice,
              "业务日期:", pDepthMarketData.ActionDay,
              )
    def OnRspUserLogin(self, pRspUserLogin: mdapi.CThostFtdcRspUserLoginField,
                       pRspInfo: mdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 登录应答 """
        if pRspInfo and pRspInfo.ErrorID:
            print("登录失败: ErrorID=", pRspInfo.ErrorID, "ErrorMsg=", pRspInfo.ErrorMsg)
            return

        print("登录成功: ErrorID=", pRspInfo.ErrorID, "ErrorMsg=", pRspInfo.ErrorMsg)

        if len(instruments) == 0:
            print("无订阅合约 !!! ")
            return

        # 订阅行情请求
        print("订阅合约行情请求:", instruments)
        self._api.SubscribeMarketData([i.encode('utf-8') for i in instruments], len(instruments))
    def OnRspSubMarketData(self, pSpecificInstrument: mdapi.CThostFtdcSpecificInstrumentField,
                           pRspInfo: mdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 订阅行情应答 """
        if pRspInfo and pRspInfo.ErrorID:
            print("订阅行情失败: ErrorID=", pRspInfo.ErrorID, "ErrorMsg=", pRspInfo.ErrorMsg)
            return

        print("订阅行情成功: InstrumentID=", pSpecificInstrument.InstrumentID)


if __name__ == '__main__':
    # 行情前置地址 openctp 7x24 环境
    md_front = 'tcp://121.37.80.177:20004'
    # 订阅合约 (确保是有效的合约)
    instruments = ('FG309',)

    # 实例化请求类
    api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi("market")  # type: mdapi.CThostFtdcMdApi
    print("TTS行情API版本号:", api.GetApiVersion())
    # 实例化回调实现类
    spi = CMdSpiImpl(api)
    # 请求实例注册行情前置地址
    api.RegisterFront(md_front)
    # 请求实例注册回调实例
    api.RegisterSpi(spi)
    # 初始化请求实例
    api.Init()

    print("Press Enter key to exit ...")
    # 阻塞主线程，等待 连接、认证、登录、订阅合约、接收行情
    input()

    # 释放请求类
    api.Release()