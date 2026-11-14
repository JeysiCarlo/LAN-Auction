import queue
import sys
import threading
from python_banyan.banyan_base import BanyanBase

class AuctionServer(BanyanBase):
    def __init__(self):
        self.admin_commands = queue.Queue()
        self.items = {}
        self.next_item_id = 1
        self.active_item_id = None
        super().__init__(
            process_name = "Auction Server",
            receive_loop_idle_addition = self.process_admin_commands
        )
        self.set_subscriber_topic("auction/bid")
        self.set_subscriber_topic("auction/item/submit")
        admin_thread = threading.Thread(
            target = self.admin_input_loop,
            daemon = True
        )
        admin_thread.start()
        print("Auction Server is running...")
        try:
            self.receive_loop()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            self.clean_up()
            sys.exit(0)
    def admin_input_loop(self):
        print("\n ----Auction Admin-----")
        print("Commands:")
        print("list")
        print("approve <Id>")
        print("reject <Id>")
        print("start <Id>")
        print("stop")
        print("help")
        print("--------------------------------")
        while True:
            command = input("\nAdmin> ").strip()
            self.admin_commands.put(command)
    def process_admin_commands(self):
        while not self.admin_commands.empty():
            command = self.admin_commands.get()
            parts = command.split()
            if not parts:
                continue
            action = parts[0].lower()
            if action == "list":
                if not self.items:
                    print("No items submitted for auction.")
                    continue
                print("\n-----Item List-----")
                for item_id, item in self.items.items():
                    print("\nID:", item_id)
                    print("Item:", item["Item"])
                    print("Seller:", item["Seller"])
                    print("Starting Price(PHP):", item["StartingPrice"])
                    print("Status:", item["Status"])
            elif action in ["approve", "reject", "start"]:
                if len(parts) != 2 or not parts[1].isdigit():
                    print("Usage:", action, "<Id>")
                    continue
                item_id = int(parts[1])
                if item_id not in self.items:
                    print("Item not found.")
                    continue
                item = self.items[item_id]
                if action == "approve":
                    if item["Status"] != "pending":
                        print("Only pending items can be approved.")
                        continue
                    item["Status"] = "approved"
                    print("Items approved:", item["Name"])
                elif action == "reject":
                    if item["Status"] != "pending":
                        print("Only pending items can be rejected.")
                        continue
                    item["Status"] = "rejected"
                    print("Items rejected:", item["Name"])
                elif action == "start": 
                    if item["Status"] != "approved":
                        print("Approve the item before starting the auction.")
                        continue
                    if self.active_item_id is not None:
                        print("Another auction is already active")
                        continue
                    item["Status"] = "active"
                    item["CurrentPrice"] = item["StartingPrice"]
                    item["HighestBidder"] = None
                    self.active_item_id = item_id
                    self.publish_payload(item, "auction/item/update")
                    print("\nAuction Started!")
                    print("Item:", item["Name"])
            elif action == "stop":
                if self.active_item_id is None:
                    print("No active auction to stop.")
                    continue
                item = self.items[self.active_item_id]
                item["Status"] = "completed"
                if item["HighestBidder"] is not None:
                    print("Winner:", item["HighestBidder"])
                    print("Winning Bid(PHP):", item["CurrentPrice"])
                else:
                    print("No bids were placed for this item.")
                print("------------------------------------------")
                self.publish_payload(item, "auction/item/update")
                self.active_item_id = None
            elif action == "help":
                print("\nAvailable commands:")
                print("list")
                print("approve <ID>")
                print("reject <ID>")
                print("start <ID>")
                print("stop")
            else:
                print("Unknown command. Type help")
    def incoming_message_processing(self, topic, payload):
        print("\n--Incoming message received--")
        print("Topic:", topic)
        print("Payload:", repr(payload))
        print("Payload type:", type(payload).__name__)
        if topic == "auction/item/submit":
            if not isinstance(payload, dict):
                print("Invalid item submission")
                return
            required_fields = [
                "Seller",
                "Name",
                "Description",
                "StartingPrice"
            ]
            if not all(field in payload for field in required_fields):
                print("Invalid item submission")
                return
            try:
                starting_price = float(payload["StartingPrice"])
                if starting_price <= 0:
                    print("Starting price must be greater than zero.")
                    return
            except ValueError:
                print("Invalid starting price format.")
                return
            item_id = self.next_item_id
            self.next_item_id += 1
            item = {
                "ID": item_id,
            "Seller": str(payload["Seller"]),
            "Name": str(payload["Name"]),
            "Description": str(payload["Description"]),
            "StartingPrice": starting_price,
            "Status": "pending"
            }
            self.items[item_id] = item
            print("\n========== NEW ITEM ==========")
            print("Item ID:", item_id)
            print("Item:", item["Name"])
            print("Seller:", item["Seller"])
            print("Starting Price: PHP", item["StartingPrice"])
            print("Status: Pending approval")
            print("==============================")
        if topic == "auction/bid":
            if self.active_item_id is None:
                print("Bid rejected: There is no active auction.")
                return
            if not isinstance(payload, dict):
                print("Invalid bid submission")
                return
            if "Bider" not in payload or "Amount" not in payload:
                print("Invalid bid submission")
                return
            if self.active_item_id is None:
                print("No active auction. Bid rejected.")
                return
            bidder = str(payload["Bider"]).strip()
            if not bidder:
                print("Bidder name cannot be empty. Bid rejected.")
                return
            try:
                amount = float(payload["Amount"])
            except (ValueError, TypeError):
                print("Bid rejected: Invalid bid amount.")
            if amount <= 0:
                print("Bid rejected: Amount must be not zero or lower than it")
                return
            item = self.items[self.active_item_id]
            if amount <= item["CurrentPrice"]:
                print("Bid rejected: you must bid higher than the current bid.", item["CurrentBid"])
                return
            item["CurrentPrice"] = amount
            item["HighestBidder"] = bidder
            bid_update = {
                "ItemID": self.active_item_id,
                "ItemName": item['Name'],
                "Bidder": bidder,
                "Amount": amount
            }
            print("\n========== NEW HIGHEST BID ==========")
            print("Item:", item["Name"])
            print("Bidder:", bidder)
            print("Amount: PHP", amount)
            print("=====================================")
            self.publish_payload(bid_update, "auction/update")
if __name__ == "__main__":
    AuctionServer()
