import asyncio
from binance import AsyncClient, BinanceSocketManager




async def calculate_liquidity_metrics(depth_data): 

    bids = [[float(price), float(qty)] for price, qty in depth_data['bids']]
    asks = [[float(price), float(qty)] for price, qty in depth_data['asks']]


    best_bid_price = bids[0][0] 
    best_bid_qty = bids[0][1]
    best_ask_price = asks[0][0] 
    best_ask_qty = asks[0][1]


    #basic metrics

    spread = best_ask_price - best_bid_price
    mid_price = (best_bid_price + best_ask_price) / 2
    spread_percentage = (spread / mid_price) * 100 if mid_price != 0 else 0

    #total liquidity in top 10 levels

    total_bid_volume = sum(qty for price, qty in bids)
    total_ask_volume = sum(qty for price, qty in asks)

    # Order book imbalance

    total_volume = total_bid_volume + total_ask_volume
    imbalance = (total_bid_volume - total_ask_volume) / total_volume if total_volume != 0 else 0
    metrics = {
        'best_bid_price': best_bid_price,
        'best_bid_qty': best_bid_qty,
        'best_ask_price': best_ask_price,
        'best_ask_qty': best_ask_qty,
        'spread': spread,
        'mid_price': mid_price,
        'spread_percentage': spread_percentage,
        'total_bid_volume': total_bid_volume,
        'total_ask_volume': total_ask_volume,
        'order_book_imbalance': imbalance
    }


    return metrics



async def main():

    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)

    # connect to depth stream for BTCUSDT


    depth_socket = bsm.depth_socket('BTCUSDT', depth=10)

    async with depth_socket as stream:
        print("Monitoring BTCUSDT Liquidity...")

        print('-' * 81)

        count = 0

        while count < 10:  # limit to 10 updates for demonstration

            msg = await stream.recv()
            metrics = await calculate_liquidity_metrics(msg)

            print(f"Mid Price: ${metrics['mid_price']:.2f} | Spread: ${metrics['spread']:.3f} | total Bid Volume: {metrics['total_bid_volume']:.4f} | total Ask Volume: {metrics['total_ask_volume']:.4f} | Order Book Imbalance: {metrics['order_book_imbalance']:.4f}")
            count += 1

            await asyncio.sleep(1)  # slight delay to avoid overwhelming output

    await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())

