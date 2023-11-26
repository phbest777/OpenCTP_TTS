"""
written by krenx on 2023-1-10.
published in openctp@github: https://github.com/krenx1983/openctp/tree/master/ctpapi-python
"""
import inspect
import cx_Oracle
import sys

# import thosttraderapi  as api
from openctp_tts import thosttraderapi as api
from openctp_tts import mdapi


class CTdSpiImpl(api.CThostFtdcTraderSpi):
    def __init__(self, tdapi):
        super().__init__()
        self.tdapi = tdapi

    def OnFrontConnected(self):
        """ 前置连接成功 """
        print("OnFrontConnected")
        req = api.CThostFtdcReqAuthenticateField()
        req.BrokerID = brokerid
        req.UserID = user
        req.AppID = appid
        req.AuthCode = authcode
        self.tdapi.ReqAuthenticate(req, 0)

    def OnFrontDisconnected(self, nReason: int):
        """ 前置断开 """
        print("OnFrontDisconnected: nReason=", nReason)

    def OnRspAuthenticate(self, pRspAuthenticateField: api.CThostFtdcRspAuthenticateField,
                          pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 客户端认证应答 """
        if pRspInfo is not None:
            print(f"OnRspAuthenticate: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")

        if pRspInfo is None or pRspInfo.ErrorID == 0:
            req = api.CThostFtdcReqUserLoginField()
            req.BrokerID = brokerid
            req.UserID = user
            req.Password = password
            req.UserProductInfo = "openctp"
            self.tdapi.ReqUserLogin(req, 0)

    def OnRspUserLogin(self, pRspUserLogin: api.CThostFtdcRspUserLoginField,
                       pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 登录应答 """
        print(f"OnRspUserLogin: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}, "
              f"TradingDay={pRspUserLogin.TradingDay}")

        req = api.CThostFtdcSettlementInfoConfirmField()
        req.BrokerID = brokerid
        req.InvestorID = user
        self.tdapi.ReqSettlementInfoConfirm(req, 0)

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm: api.CThostFtdcSettlementInfoConfirmField,
                                   pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 确认投资者结算结果应答 """
        if pRspInfo is not None:
            print(f"OnRspSettlementInfoConfirm: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")

        req = api.CThostFtdcQryInstrumentField()
        req.BrokerID = brokerid
        req.InvestorID = user
        self.tdapi.ReqQryInstrument(req, 0)

    def OnRspQryInstrument(self, pInstrument: api.CThostFtdcInstrumentField,
                           pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 查询合约应答 """
        if pRspInfo is not None:
            print(f"OnRspQryInstrument: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")
        '''
        params = []
        for name, value in inspect.getmembers(pInstrument):
            if name[0].isupper():
                params.append(f"{name}={value}")
        print("深度行情通知:", ",".join(params))
        '''


        print(f"OnRspQryInstrument: InstrumentID={pInstrument.InstrumentID}, DeliverYear={pInstrument.DeliveryYear},"
              f"ExchangeID={pInstrument.ExchangeID}, PriceTick={pInstrument.PriceTick}, OptionsType={pInstrument.OptionsType}"
              f"ProductID={pInstrument.ProductID}, ExpireDate={pInstrument.ExpireDate}")
        sql = "insert into QUANT_FUTURE_INSTRUMENT (EXCHANGEID,INSTRUMENTNAME,PRODUCTCLASS,DELIVERYYEAR,DELIVERYMONTH,MAXMARKETORDERVOLUME" \
              ",MINMARKETORDERVOLUME,MAXLIMITORDERVOLUME,MINLIMITORDERVOLUME,VOLUMEMULTIPLE,PRICETICK,CREATEDATE,OPENDATE,EXPIREDATE,STARTDELIVDATE" \
              ",ENDDELIVDATE,INSTLIFEPHASE,ISTRADING,POSITIONTYPE,POSITIONDATETYPE,LONGMARGINRATIO,SHORTMARGINRATIO,MAXMARGINSIDEALGORITHM" \
              ",STRIKEPRICE,OPTIONSTYPE,UNDERLYINGMULTIPLE,COMBINATIONTYPE,INSTRUMENTID,EXCHANGEINSTID,PRODUCTID,UNDERLYINGINSTRID)values(" \
              "'" + pInstrument.ExchangeID + "','" + pInstrument.InstrumentName + "','" + pInstrument.ProductClass + \
              "','" + str(pInstrument.DeliveryYear) +"'," +"lpad('"+ str(pInstrument.DeliveryMonth)+"',2,"+"'0')" +\
              "," + str(pInstrument.MaxMarketOrderVolume) +","+str(pInstrument.MinMarketOrderVolume) +","+str(pInstrument.MaxLimitOrderVolume) +\
              ","+str(pInstrument.MinLimitOrderVolume) +","+str(pInstrument.VolumeMultiple) +","+str(pInstrument.PriceTick) +",'" + str(pInstrument.CreateDate) +\
              "','"+ str(pInstrument.OpenDate)+"','"+ str(pInstrument.ExpireDate)+"','"+ str(pInstrument.StartDelivDate)+"','"+ str(pInstrument.EndDelivDate)+"','"+\
              str(pInstrument.InstLifePhase)+"','"+ str(pInstrument.IsTrading)+"','"+ str(pInstrument.PositionType)+"','"+ str(pInstrument.PositionDateType)+\
              "',"+str(pInstrument.LongMarginRatio)+","+str(pInstrument.ShortMarginRatio)+",'"+pInstrument.MaxMarginSideAlgorithm+"',"+str(pInstrument.StrikePrice)+\
              ",'"+str(pInstrument.OptionsType)+"'"+","+str(pInstrument.UnderlyingMultiple)+",'"+str(pInstrument.CombinationType)+"','"+str(pInstrument.InstrumentID)+\
              "','"+str(pInstrument.ExchangeInstID)+"','"+str(pInstrument.ProductID)+"','"+str(pInstrument.UnderlyingInstrID)+"')"
        #print("sqlstr is:" + sql)
        cursor.execute(sql)
        conn.commit()

    def OnRtnDepthMarketData(self, pDepthMarketData: api.CThostFtdcDepthMarketDataField):
        """ 深度行情 """
        print("InstrumentID:", pDepthMarketData.InstrumentID, " LastPrice:", pDepthMarketData.LastPrice,
              " Volume:", pDepthMarketData.Volume, " PreSettlementPrice:", pDepthMarketData.PreSettlementPrice,
              " PreClosePrice:", pDepthMarketData.PreClosePrice, " TradingDay:", pDepthMarketData.TradingDay)

    def OnRspSubMarketData(self, pSpecificInstrument: api.CThostFtdcSpecificInstrumentField,
                           pRspInfo: mdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 订阅行情应答 """
        print("OnRspSubMarketData:ErrorID=", pRspInfo.ErrorID, " ErrorMsg=", pRspInfo.ErrorMsg)

    def OnRtnOrder(self, pOrder: api.CThostFtdcOrderField):
        """ 报单回报 """
        print(f"OnRtnOrder: InstrumentID={pOrder.InstrumentID}, ExchangeID={pOrder.ExchangeID}, "
              f"VolumeTotalOriginal={pOrder.VolumeTotalOriginal}, VolumeTraded={pOrder.VolumeTraded}, "
              f"LimitPrice={pOrder.LimitPrice}, OrderStatus={pOrder.OrderStatus}, OrderSysID={pOrder.OrderSysID}, "
              f"FrontID={pOrder.FrontID}, SessionID={pOrder.SessionID}, OrderRef={pOrder.OrderRef}")

    def OnRtnTrade(self, pTrade: api.CThostFtdcTradeField):
        """ 成交回报 """
        print(f"OnRtnTrade: InstrumentID={pTrade.InstrumentID}, ExchangeID={pTrade.ExchangeID}, "
              f"Volume={pTrade.Volume}, Price={pTrade.Price}, OrderSysID={pTrade.OrderSysID}, "
              f"OrderRef={pTrade.OrderRef}")

    def OnRspOrderInsert(self, pInputOrder: api.CThostFtdcInputOrderField,
                         pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 报单响应 """
        if pRspInfo is not None:
            print(f"OnRspOrderInsert: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")


if __name__ == '__main__':
    '''if len(sys.argv) != 4:
        print("Usage:\n\tpython ctpprint.py <front_addr> <investorId> <password>")
        sys.exit(1)'''

    tdfront = 'tcp://121.37.80.177:20004'
    user = '7707'
    password = '123456'

    brokerid = '9999'
    authcode = '0000000000000000'
    appid = 'simnow_client_test'

    conn = cx_Oracle.connect('user_ph', 'ph', '127.0.1.1:1521/orclpdb')
    cursor = conn.cursor()
    print('连接数据库成功！')
    val = ("55555")

    # instruments = ('SA401',)
    tdapi = api.CThostFtdcTraderApi.CreateFtdcTraderApi("data\\auth\\" + user)
    print("ApiVersion: ", tdapi.GetApiVersion())
    tdspi = CTdSpiImpl(tdapi)
    tdapi.RegisterSpi(tdspi)
    tdapi.SubscribePrivateTopic(api.THOST_TERT_QUICK)
    tdapi.SubscribePublicTopic(api.THOST_TERT_QUICK)
    tdapi.RegisterFront(tdfront)
    tdapi.Init()

    print("press Enter key to exit ...")
    input()
