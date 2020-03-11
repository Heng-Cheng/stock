import crawler_investor
import crawler_margin
import crawler_margin_financing
import crawler_futures
import stock50

if __name__ == '__main__':
    d = stock50.Daily_trade_data()
    d.update_data()
    TAI = stock50.TAIEX()
    TAI.update_data()
    crawler_investor.crawler_and_update()
    crawler_margin.crawler_and_update()
    crawler_margin_financing.crawler_and_update()
    crawler_futures.update_futures()