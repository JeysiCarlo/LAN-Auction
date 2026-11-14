from python_banyan.banyan_base import BanyanBase
import sys
import queue
import threading
class AuctionClient(BanyanBase):
    def __init__(self, bidder):
        self.bid_queue = queue.Queue()
        self.bidder = bidder
        super().__init__(
            process_name = "Auction Client",
            receive_loop_idle_addition = self.process_pending_bids
        )
        self.set_subscriber_topic("auction/update")
        print("\nWelcome to the Auction", self.bidder)
        print("You can now enter multiple bids")
        print("Press Ctrl C to exit the auction.\n")
        input_thread = threading.Thread(
            target = self.get_bid_amounts,
            daemon = True
        )
        input_thread.start()
        try:
            self.receive_loop()
        except KeyboardInterrupt:
            print("\nShutting down client...")
            sys.exit(0)
    def get_bid_amounts(self):
        while True:
            try:
                amount = float(input("Enter your bid amount(PHP): "))
                if amount <= 0:
                    print("Bid amount must be greater than zero.")
                    continue
                self.bid_queue.put(amount)
            except ValueError:
                print("Invalid input. Please enter a valid number.")
    def process_pending_bids(self):
        while not self.bid_queue.empty():
            amount = self.bid_queue.get()
            bid = {
                "Bidder": self.bidder,
                "Amount": amount
            }
            self.publish_payload(bid, "auction/bid")
            print("\nYour bid has been submitted.")
    def incoming_message_processing(self, topic, payload):
        if topic == "auction/update":
            print("\n--Auction Update--")
            print("Item:", payload["ItemName"])
            print("Bidder:", payload["Bidder"])
            print("Amount(PHP):", payload["Amount"])
            print("-------------------------------")
def get_bidder_name():
    print("-----------------------------------")
    print("          Auction Client")
    print("-----------------------------------")
    bidder = input("Enter your name: ").strip()
    while not bidder:
        print("Name cannot be empty. Please enter a valid name.")
        bidder = input("Enter your name: ").strip()
    return bidder
if __name__ == "__main__":
    bidder = get_bidder_name()
    AuctionClient(bidder)