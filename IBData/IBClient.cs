using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using IBApi;
using IB.messages;
using System.Threading;
using System.Threading.Tasks;

namespace IB
{
    class IBClient : EWrapper
    {
        private EClientSocket clientSocket;
        private int nextOrderId;
        private int clientId;

        public Task<Contract> ResolveContractAsync(int conId, string refExch)
        {
            var reqId = new Random(DateTime.Now.Millisecond).Next();
            var resolveResult = new TaskCompletionSource<Contract>();
            var resolveContract_Error = new Action<int, int, string, Exception>((id, code, msg, ex) =>
                {
                    if (reqId != id)
                        return;

                    resolveResult.SetResult(null);
                });
            var resolveContract = new Action<ContractDetailsMessage>(msg =>
                {
                    if (msg.RequestId == reqId)
                        resolveResult.SetResult(msg.ContractDetails.Contract);
                });
            var contractDetailsEnd = new Action<int>(id =>
            {
                if (reqId == id && !resolveResult.Task.IsCompleted)
                    resolveResult.SetResult(null);
            });

            var tmpError = Error;
            var tmpContractDetails = ContractDetails;
            var tmpContractDetailsEnd = ContractDetailsEnd;

            Error = resolveContract_Error;
            ContractDetails = resolveContract;
            ContractDetailsEnd = contractDetailsEnd;

            resolveResult.Task.ContinueWith(t =>
            {
                Error = tmpError;
                ContractDetails = tmpContractDetails;
                ContractDetailsEnd = tmpContractDetailsEnd;
            });

            ClientSocket.reqContractDetails(reqId, new Contract() { ConId = conId, Exchange = refExch });

            return resolveResult.Task;
        }

        public Task<Contract[]> ResolveContractAsync(string secType, string symbol, string currency, string exchange)
        {
            var reqId = new Random(DateTime.Now.Millisecond).Next();
            var res = new TaskCompletionSource<Contract[]>();
            var contractList = new List<Contract>();
            var resolveContract_Error = new Action<int, int, string, Exception>((id, code, msg, ex) =>
                {
                    if (reqId != id)
                        return;

                    res.SetResult(new Contract[0]);
                });
            var contractDetails = new Action<ContractDetailsMessage>(msg =>
                {
                    if (reqId != msg.RequestId)
                        return;

                    contractList.Add(msg.ContractDetails.Contract);
                });
            var contractDetailsEnd = new Action<int>(id =>
                {
                    if (reqId == id)
                        res.SetResult(contractList.ToArray());
                });

            var tmpError = Error;
            var tmpContractDetails = ContractDetails;
            var tmpContractDetailsEnd = ContractDetailsEnd;

            Error = resolveContract_Error;
            ContractDetails = contractDetails;
            ContractDetailsEnd = contractDetailsEnd;

            res.Task.ContinueWith(t =>
            {
                Error = tmpError;
                ContractDetails = tmpContractDetails;
                ContractDetailsEnd = tmpContractDetailsEnd;
            });

            ClientSocket.reqContractDetails(reqId, new Contract() { SecType = secType, Symbol = symbol, Currency = currency, Exchange = exchange });

            return res.Task;
        }

        public int ClientId
        {
            get { return clientId; }
            set { clientId = value; }
        }

        SynchronizationContext sc;

        public IBClient(EReaderSignal signal)
        {
            clientSocket = new EClientSocket(this, signal);
            sc = SynchronizationContext.Current;
        }

        public EClientSocket ClientSocket
        {
            get { return clientSocket; }
            private set { clientSocket = value; }
        }

        public int NextOrderId
        {
            get { return nextOrderId; }
            set { nextOrderId = value; }
        }

        public event Action<int, int, string, Exception> Error;

        void EWrapper.error(Exception e)
        {
            Error?.Invoke(0, 0, null, e);
        }

        void EWrapper.error(string str)
        {
            Error?.Invoke(0, 0, str, null);
        }

        void EWrapper.error(int id, int errorCode, string errorMsg)
        {
            Error?.Invoke(id, errorCode, errorMsg, null);
        }

        public event Action ConnectionClosed;

        void EWrapper.connectionClosed()
        {
            ConnectionClosed?.Invoke();
        }

        public event Action<long> CurrentTime;

        void EWrapper.currentTime(long time)
        {
            CurrentTime?.Invoke(time);
        }

        public event Action<TickPriceMessage> TickPrice;

        void EWrapper.tickPrice(int tickerId, int field, double price, TickAttrib attribs)
        {
            TickPrice?.Invoke(new TickPriceMessage(tickerId, field, price, attribs));
        }

        public event Action<TickSizeMessage> TickSize;

        void EWrapper.tickSize(int tickerId, int field, int size)
        {
            TickSize?.Invoke(new TickSizeMessage(tickerId, field, size));
        }

        public event Action<int, int, string> TickString;

        void EWrapper.tickString(int tickerId, int tickType, string value)
        {
            TickString?.Invoke(tickerId, tickType, value);
        }

        public event Action<int, int, double> TickGeneric;

        void EWrapper.tickGeneric(int tickerId, int field, double value)
        {
            TickGeneric?.Invoke(tickerId, field, value);
        }

        public event Action<int, int, double, string, double, int, string, double, double> TickEFP;

        void EWrapper.tickEFP(int tickerId, int tickType, double basisPoints, string formattedBasisPoints, double impliedFuture, int holdDays, string futureLastTradeDate, double dividendImpact, double dividendsToLastTradeDate)
        {
            TickEFP?.Invoke(tickerId, tickType, basisPoints, formattedBasisPoints, impliedFuture, holdDays, futureLastTradeDate, dividendImpact, dividendsToLastTradeDate);
        }

        public event Action<int> TickSnapshotEnd;

        void EWrapper.tickSnapshotEnd(int tickerId)
        {
            TickSnapshotEnd?.Invoke(tickerId);
        }

        public event Action<ConnectionStatusMessage> NextValidId;

        void EWrapper.nextValidId(int orderId)
        {
            NextValidId?.Invoke(new ConnectionStatusMessage(true));
            NextOrderId = orderId;
        }

        public event Action<int, DeltaNeutralContract> DeltaNeutralValidation;

        void EWrapper.deltaNeutralValidation(int reqId, DeltaNeutralContract deltaNeutralContract)
        {
            DeltaNeutralValidation?.Invoke(reqId, deltaNeutralContract);
        }

        public event Action<ManagedAccountsMessage> ManagedAccounts;

        void EWrapper.managedAccounts(string accountsList)
        {
            ManagedAccounts?.Invoke(new ManagedAccountsMessage(accountsList));
        }

        public event Action<TickOptionMessage> TickOptionCommunication;

        void EWrapper.tickOptionComputation(int tickerId, int field, double impliedVolatility, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice)
        {
            TickOptionCommunication?.Invoke(new TickOptionMessage(tickerId, field, impliedVolatility, delta, optPrice, pvDividend, gamma, vega, theta, undPrice));
        }

        public event Action<AccountSummaryMessage> AccountSummary;

        void EWrapper.accountSummary(int reqId, string account, string tag, string value, string currency)
        {
            AccountSummary?.Invoke(new AccountSummaryMessage(reqId, account, tag, value, currency));
        }

        public event Action<AccountSummaryEndMessage> AccountSummaryEnd;

        void EWrapper.accountSummaryEnd(int reqId)
        {
            AccountSummaryEnd?.Invoke(new AccountSummaryEndMessage(reqId));
        }

        public event Action<AccountValueMessage> UpdateAccountValue;

        void EWrapper.updateAccountValue(string key, string value, string currency, string accountName)
        {
            UpdateAccountValue?.Invoke(new AccountValueMessage(key, value, currency, accountName));
        }

        public event Action<UpdatePortfolioMessage> UpdatePortfolio;

        void EWrapper.updatePortfolio(Contract contract, double position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, string accountName)
        {
            UpdatePortfolio?.Invoke(new UpdatePortfolioMessage(contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName));
        }

        public event Action<UpdateAccountTimeMessage> UpdateAccountTime;

        void EWrapper.updateAccountTime(string timestamp)
        {
            UpdateAccountTime?.Invoke(new UpdateAccountTimeMessage(timestamp));
        }

        public event Action<AccountDownloadEndMessage> AccountDownloadEnd;

        void EWrapper.accountDownloadEnd(string account)
        {
            AccountDownloadEnd?.Invoke(new AccountDownloadEndMessage(account));
        }

        public event Action<OrderStatusMessage> OrderStatus;

        void EWrapper.orderStatus(int orderId, string status, double filled, double remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, string whyHeld, double mktCapPrice)
        {
            OrderStatus?.Invoke(new OrderStatusMessage(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice));
        }

        public event Action<OpenOrderMessage> OpenOrder;

        void EWrapper.openOrder(int orderId, Contract contract, Order order, OrderState orderState)
        {
            OpenOrder?.Invoke(new OpenOrderMessage(orderId, contract, order, orderState));
        }

        public event Action OpenOrderEnd;

        void EWrapper.openOrderEnd()
        {
            OpenOrderEnd?.Invoke();
        }

        public event Action<ContractDetailsMessage> ContractDetails;

        void EWrapper.contractDetails(int reqId, ContractDetails contractDetails)
        {
            ContractDetails?.Invoke(new ContractDetailsMessage(reqId, contractDetails));
        }

        public event Action<int> ContractDetailsEnd;

        void EWrapper.contractDetailsEnd(int reqId)
        {
            ContractDetailsEnd?.Invoke(reqId);
        }

        public event Action<ExecutionMessage> ExecDetails;

        void EWrapper.execDetails(int reqId, Contract contract, Execution execution)
        {
            ExecDetails?.Invoke(new ExecutionMessage(reqId, contract, execution));
        }

        public event Action<int> ExecDetailsEnd;

        void EWrapper.execDetailsEnd(int reqId)
        {
            ExecDetailsEnd?.Invoke(reqId);
        }

        public event Action<CommissionReport> CommissionReport;

        void EWrapper.commissionReport(CommissionReport commissionReport)
        {
            CommissionReport?.Invoke(commissionReport);
        }

        public event Action<FundamentalsMessage> FundamentalData;

        void EWrapper.fundamentalData(int reqId, string data)
        {
            FundamentalData?.Invoke(new FundamentalsMessage(data));
        }

        public event Action<HistoricalDataMessage> HistoricalData;

        void EWrapper.historicalData(int reqId, Bar bar)
        {
            HistoricalData?.Invoke(new HistoricalDataMessage(reqId, bar));
        }

        public event Action<HistoricalDataEndMessage> HistoricalDataEnd;

        void EWrapper.historicalDataEnd(int reqId, string startDate, string endDate)
        {
            HistoricalDataEnd?.Invoke(new HistoricalDataEndMessage(reqId, startDate, endDate));
        }

        public event Action<MarketDataTypeMessage> MarketDataType;

        void EWrapper.marketDataType(int reqId, int marketDataType)
        {
            MarketDataType?.Invoke(new MarketDataTypeMessage(reqId, marketDataType));
        }

        public event Action<DeepBookMessage> UpdateMktDepth;

        void EWrapper.updateMktDepth(int tickerId, int position, int operation, int side, double price, int size)
        {
            UpdateMktDepth?.Invoke(new DeepBookMessage(tickerId, position, operation, side, price, size, "", false));
        }

        public event Action<DeepBookMessage> UpdateMktDepthL2;

        void EWrapper.updateMktDepthL2(int tickerId, int position, string marketMaker, int operation, int side, double price, int size, bool isSmartDepth)
        {
            UpdateMktDepthL2?.Invoke(new DeepBookMessage(tickerId, position, operation, side, price, size, marketMaker, isSmartDepth));
        }

        public event Action<int, int, String, String> UpdateNewsBulletin;

        void EWrapper.updateNewsBulletin(int msgId, int msgType, String message, String origExchange)
        {
            UpdateNewsBulletin?.Invoke(msgId, msgType, message, origExchange);
        }

        public event Action<PositionMessage> Position;

        void EWrapper.position(string account, Contract contract, double pos, double avgCost)
        {
            Position?.Invoke(new PositionMessage(account, contract, pos, avgCost));
        }

        public event Action PositionEnd;

        void EWrapper.positionEnd()
        {
            PositionEnd?.Invoke();
        }

        public event Action<RealTimeBarMessage> RealtimeBar;

        void EWrapper.realtimeBar(int reqId, long time, double open, double high, double low, double close, long volume, double WAP, int count)
        {
            RealtimeBar?.Invoke(new RealTimeBarMessage(reqId, time, open, high, low, close, volume, WAP, count));
        }

        public event Action<string> ScannerParameters;

        void EWrapper.scannerParameters(string xml)
        {
            ScannerParameters?.Invoke(xml);
        }

        public event Action<ScannerMessage> ScannerData;

        void EWrapper.scannerData(int reqId, int rank, ContractDetails contractDetails, string distance, string benchmark, string projection, string legsStr)
        {
            ScannerData?.Invoke(new ScannerMessage(reqId, rank, contractDetails, distance, benchmark, projection, legsStr));
        }

        public event Action<int> ScannerDataEnd;

        void EWrapper.scannerDataEnd(int reqId)
        {
            ScannerDataEnd?.Invoke(reqId);
        }

        public event Action<AdvisorDataMessage> ReceiveFA;

        void EWrapper.receiveFA(int faDataType, string faXmlData)
        {
            ReceiveFA?.Invoke(new AdvisorDataMessage(faDataType, faXmlData));
        }

        public event Action<BondContractDetailsMessage> BondContractDetails;

        void EWrapper.bondContractDetails(int requestId, ContractDetails contractDetails)
        {
            BondContractDetails?.Invoke(new BondContractDetailsMessage(requestId, contractDetails));
        }

        public event Action<string> VerifyMessageAPI;

        void EWrapper.verifyMessageAPI(string apiData)
        {
            VerifyMessageAPI?.Invoke(apiData);
        }

        public event Action<bool, string> VerifyCompleted;

        void EWrapper.verifyCompleted(bool isSuccessful, string errorText)
        {
            VerifyCompleted?.Invoke(isSuccessful, errorText);
        }

        public event Action<string, string> VerifyAndAuthMessageAPI;

        void EWrapper.verifyAndAuthMessageAPI(string apiData, string xyzChallenge)
        {
            VerifyAndAuthMessageAPI?.Invoke(apiData, xyzChallenge);
        }

        public event Action<bool, string> VerifyAndAuthCompleted;

        void EWrapper.verifyAndAuthCompleted(bool isSuccessful, string errorText)
        {
            VerifyAndAuthCompleted?.Invoke(isSuccessful, errorText);
        }

        public event Action<int, string> DisplayGroupList;

        void EWrapper.displayGroupList(int reqId, string groups)
        {
            DisplayGroupList?.Invoke(reqId, groups);
        }

        public event Action<int, string> DisplayGroupUpdated;

        void EWrapper.displayGroupUpdated(int reqId, string contractInfo)
        {
            DisplayGroupUpdated?.Invoke(reqId, contractInfo);
        }

        void EWrapper.connectAck()
        {
            if (ClientSocket.AsyncEConnect)
                ClientSocket.startApi();
        }

        public event Action<PositionMultiMessage> PositionMulti;

        void EWrapper.positionMulti(int reqId, string account, string modelCode, Contract contract, double pos, double avgCost)
        {
            PositionMulti?.Invoke(new PositionMultiMessage(reqId, account, modelCode, contract, pos, avgCost));
        }

        public event Action<int> PositionMultiEnd;

        void EWrapper.positionMultiEnd(int reqId)
        {
            PositionMultiEnd?.Invoke(reqId);
        }

        public event Action<AccountUpdateMultiMessage> AccountUpdateMulti;

        void EWrapper.accountUpdateMulti(int reqId, string account, string modelCode, string key, string value, string currency)
        {
            AccountUpdateMulti?.Invoke(new AccountUpdateMultiMessage(reqId, account, modelCode, key, value, currency));
        }

        public event Action<int> AccountUpdateMultiEnd;

        void EWrapper.accountUpdateMultiEnd(int reqId)
        {
            AccountUpdateMultiEnd?.Invoke(reqId);
        }

        public event Action<SecurityDefinitionOptionParameterMessage> SecurityDefinitionOptionParameter;

        void EWrapper.securityDefinitionOptionParameter(int reqId, string exchange, int underlyingConId, string tradingClass, string multiplier, HashSet<string> expirations, HashSet<double> strikes)
        {
            SecurityDefinitionOptionParameter?.Invoke(new SecurityDefinitionOptionParameterMessage(reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes));
        }

        public event Action<int> SecurityDefinitionOptionParameterEnd;

        void EWrapper.securityDefinitionOptionParameterEnd(int reqId)
        {
            SecurityDefinitionOptionParameterEnd?.Invoke(reqId);
        }

        public event Action<SoftDollarTiersMessage> SoftDollarTiers;

        void EWrapper.softDollarTiers(int reqId, SoftDollarTier[] tiers)
        {
            SoftDollarTiers?.Invoke(new SoftDollarTiersMessage(reqId, tiers));
        }

        public event Action<FamilyCode[]> FamilyCodes;

        void EWrapper.familyCodes(FamilyCode[] familyCodes)
        {
            FamilyCodes?.Invoke(familyCodes);
        }

        public event Action<SymbolSamplesMessage> SymbolSamples;

        void EWrapper.symbolSamples(int reqId, ContractDescription[] contractDescriptions)
        {
            SymbolSamples?.Invoke(new SymbolSamplesMessage(reqId, contractDescriptions));
        }

        public event Action<DepthMktDataDescription[]> MktDepthExchanges;

        void EWrapper.mktDepthExchanges(DepthMktDataDescription[] depthMktDataDescriptions)
        {
            MktDepthExchanges?.Invoke(depthMktDataDescriptions);
        }

        public event Action<TickNewsMessage> TickNews;

        void EWrapper.tickNews(int tickerId, long timeStamp, string providerCode, string articleId, string headline, string extraData)
        {
            TickNews?.Invoke(new TickNewsMessage(tickerId, timeStamp, providerCode, articleId, headline, extraData));
        }

        public event Action<int, Dictionary<int, KeyValuePair<string, char>>> SmartComponents;

        void EWrapper.smartComponents(int reqId, Dictionary<int, KeyValuePair<string, char>> theMap)
        {
            SmartComponents?.Invoke(reqId, theMap);
        }

        public event Action<TickReqParamsMessage> TickReqParams;

        void EWrapper.tickReqParams(int tickerId, double minTick, string bboExchange, int snapshotPermissions)
        {
            TickReqParams?.Invoke(new TickReqParamsMessage(tickerId, minTick, bboExchange, snapshotPermissions));
        }

        public event Action<NewsProvider[]> NewsProviders;

        void EWrapper.newsProviders(NewsProvider[] newsProviders)
        {
            NewsProviders?.Invoke(newsProviders);
        }

        public event Action<NewsArticleMessage> NewsArticle;

        void EWrapper.newsArticle(int requestId, int articleType, string articleText)
        {
            NewsArticle?.Invoke(new NewsArticleMessage(requestId, articleType, articleText));
        }

        public event Action<HistoricalNewsMessage> HistoricalNews;

        void EWrapper.historicalNews(int requestId, string time, string providerCode, string articleId, string headline)
        {
            HistoricalNews?.Invoke(new HistoricalNewsMessage(requestId, time, providerCode, articleId, headline));
        }

        public event Action<HistoricalNewsEndMessage> HistoricalNewsEnd;

        void EWrapper.historicalNewsEnd(int requestId, bool hasMore)
        {
            HistoricalNewsEnd?.Invoke(new HistoricalNewsEndMessage(requestId, hasMore));
        }

        public event Action<HeadTimestampMessage> HeadTimestamp;

        void EWrapper.headTimestamp(int reqId, string headTimestamp)
        {
            HeadTimestamp?.Invoke(new HeadTimestampMessage(reqId, headTimestamp));
        }

        public event Action<HistogramDataMessage> HistogramData;

        void EWrapper.histogramData(int reqId, HistogramEntry[] data)
        {
            HistogramData?.Invoke(new HistogramDataMessage(reqId, data));
        }

        public event Action<HistoricalDataMessage> HistoricalDataUpdate;

        void EWrapper.historicalDataUpdate(int reqId, Bar bar)
        {
            HistoricalDataUpdate?.Invoke(new HistoricalDataMessage(reqId, bar));
        }

        public event Action<int, int, string> RerouteMktDataReq;

        void EWrapper.rerouteMktDataReq(int reqId, int conId, string exchange)
        {
            RerouteMktDataReq?.Invoke(reqId, conId, exchange);
        }

        public event Action<int, int, string> RerouteMktDepthReq;

        void EWrapper.rerouteMktDepthReq(int reqId, int conId, string exchange)
        {
            RerouteMktDepthReq?.Invoke(reqId, conId, exchange);
        }

        public event Action<MarketRuleMessage> MarketRule;

        void EWrapper.marketRule(int marketRuleId, PriceIncrement[] priceIncrements)
        {
            MarketRule?.Invoke(new MarketRuleMessage(marketRuleId, priceIncrements));
        }

        public event Action<PnLMessage> pnl;

        void EWrapper.pnl(int reqId, double dailyPnL, double unrealizedPnL, double realizedPnL)
        {
            pnl?.Invoke(new PnLMessage(reqId, dailyPnL, unrealizedPnL, realizedPnL));
        }

        public event Action<PnLSingleMessage> pnlSingle;

        void EWrapper.pnlSingle(int reqId, int pos, double dailyPnL, double unrealizedPnL, double realizedPnL, double value)
        {
            pnlSingle?.Invoke(new PnLSingleMessage(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value));
        }

        public event Action<HistoricalTickMessage> historicalTick;

        void EWrapper.historicalTicks(int reqId, HistoricalTick[] ticks, bool done)
        {
            ticks.ToList().ForEach(tick => historicalTick?.Invoke(new HistoricalTickMessage(reqId, tick.Time, tick.Price, tick.Size)));
        }

        public event Action<HistoricalTickBidAskMessage> historicalTickBidAsk;

        void EWrapper.historicalTicksBidAsk(int reqId, HistoricalTickBidAsk[] ticks, bool done)
        {
                ticks.ToList().ForEach(tick =>
                    historicalTickBidAsk?.Invoke(new HistoricalTickBidAskMessage(reqId, tick.Time, 0, tick.PriceBid, tick.PriceAsk, tick.SizeBid, tick.SizeAsk)));
        }

        public event Action<int, HistoricalTickLast[], bool> historicalTickLast;

        void EWrapper.historicalTicksLast(int reqId, HistoricalTickLast[] ticks, bool done)
        {
            historicalTickLast?.Invoke(reqId, ticks, done);
        }

        void EWrapper.tickByTickMidPoint(int reqId, long time, double midPoint) { }
        void EWrapper.tickByTickAllLast(int reqId, int tickType, long time, double price, int size, TickAttribLast tickAttribLast, string exchange, string specialConditions) { }
        void EWrapper.tickByTickBidAsk(int reqId, long time, double bidPrice, double askPrice, int bidSize, int askSize, TickAttribBidAsk tickAttribBidAsk) { }
        void EWrapper.orderBound(long orderId, int apiClientId, int apiOrderId) { }
    }
}
