import queue
import sys
import threading
import time
from python_banyan.banyan_base import BanyanBase
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout

class AuctionServer(BanyanBase):
    def __init__(self):
        self.admin_commands = queue.Queue()
        self.console_lock = threading.Lock()
        self.command_processed = threading.Event()
        self.command_processed.set()
        self.items = {}
        self.auction_history = {}
        self.next_item_id = 1
        self.active_item_id = None
        self.auction_duration = 30
        self.auction_deadline = None
        self.timer_thread = None
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
        try:
            with patch_stdout():
                admin_thread.start()
                print("Auction Server is running...")
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
        print("history")
        print("history <ID>")
        print("help")
        print("--------------------------------")
        print()
        while True:
            self.command_processed.wait()
            self.command_processed.clear()
            command = prompt("\nAdmin> ").strip()
            self.admin_commands.put(command)
    def process_admin_commands(self):
        processed_any = False
        while not self.admin_commands.empty():
            command = self.admin_commands.get()
            processed_any = True
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
                    print("Item:", item["Name"])
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
                    item["BidHistory"] = []
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
                self.auction_history[item["ID"]] = {
                    "ID": item["ID"],
                    "Name": item["Name"],
                    "Seller": item["Seller"],
                    "StartingPrice": item["StartingPrice"],
                    "FinalPrice": item["CurrentPrice"],
                    "Winner": item["HighestBidder"],
                    "BidHistory": item["BidHistory"].copy()
                }
                if item["HighestBidder"] is not None:
                    print("Winner:", item["HighestBidder"])
                    print("Winning Bid(PHP):", item["CurrentPrice"])
                else:
                    print("No bids were placed for this item.")
                print("------------------------------------------")
                self.publish_payload(item, "auction/item/update")
                self.active_item_id = None
            elif action == "history" and len(parts) == 1:
                if not self.auction_history:
                    print("No completed auctions yet.")
                    continue
                print("\n---------------Auction History-----------------")
                for auction_id, auction in self.auction_history.items():
                    print("\nAuction ID:", auction_id)
                    print("Item:", auction["Name"])
                    print("Seller:", auction["Seller"])
                    print("Starting Price: PHP", auction["StartingPrice"])
                    print("Final Price: PHP", auction["FinalPrice"])
                    if auction["Winner"] is not None:
                        print("Winner:", auction["Winner"])
                    else:
                        print("Winner: No bids")
                print("---------------------------------------------------")
            elif action =="history" and len(parts) == 2:
                if not parts[1].isdigit():
                    print("Usage: history <ID>")
                    continue
                auction_id = int(parts[1])
                if auction_id not in self.auction_history:
                    print("Auction history not found.")
                    continue
                auction = self.auction_history[auction_id]
                print("\n========== AUCTION DETAILS ==========")
                print("Auction ID:", auction["ID"])
                print("Item:", auction["Name"])
                print("Seller:", auction["Seller"])
                print("Starting Price: PHP", auction["StartingPrice"])
                print("Final Price: PHP", auction["FinalPrice"])
                if auction["Winner"] is not None:
                    print("Winner:", auction["Winner"])
                else:
                    print("Winner: No bids.")
                print("\n--------------------- Bid History-------------------")
                if not auction["BidHistory"]:
                    print("No bids were placed.")
                else:
                    for number, bid in enumerate(auction["BidHistory"], start = 1):
                        print(
                            number, 
                            "| Bidder", bid["Bidder"],
                            "| Amount: PHP", bid["Amount"]
                        )
                print("------------------------------------------------------")
            elif action == "help":
                print("\nAvailable commands:")
                print("list")
                print("approve <ID>")
                print("reject <ID>")
                print("start <ID>")
                print("stop")
                print("History")
                print("History <ID>")
            else:
                print("Unknown command. Type help")
        if processed_any:
            self.command_processed.set()
    def incoming_message_processing(self, topic, payload):
        with self.console_lock:
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
            if not isinstance(payload, dict):
                print("Invalid bid: payload is not a dictionary.")
                return
            client_id = payload.get("ClientID")
            if not client_id:
                print("Invalid bid: missing ClientID.")
                return
            if self.active_item_id is None:
                message = "There is no active auction."
                print("Bid rejected:", message)
                self.send_bid_response(client_id, False, message)
                return
            if "Bidder" not in payload or "Amount" not in payload:
                message = "Missing Bidder or Amount."
                print("Bid rejected:", message)
                self.send_bid_response(client_id, False, message)
                return
            bidder = str(payload["Bidder"]).strip()
            if not bidder:
                message = "Bidder name cannot be empty."
                print("Bid rejected:", message)
                self.send_bid_response(client_id, False, message)
                return
            try:
                amount = float(payload["Amount"])
            except (ValueError, TypeError):
                message = "Invalid bid amount."
                print("Bid rejected:", message)
                self.send_bid_response(client_id, False, message)
                return
            if amount <= 0:
                message = "Bid amount must be greater than zero."
                print("Bid rejected:", message)
                self.send_bid_response(client_id, False, message)
                return
            item = self.items[self.active_item_id]
            if amount <= item["CurrentPrice"]:
                message = f"You must bid higher than the current bid of PHP {item['CurrentPrice']}."
                print("Bid rejected:", message)
                self.send_bid_response(client_id, False, message)
                return
            item["CurrentPrice"] = amount
            item["HighestBidder"] = bidder
            item["BidHistory"].append({
                "Bidder": bidder,
                "Amount": amount
            })
            bid_update = {
                "ItemID": self.active_item_id,
                "ItemName": item["Name"],
                "Bidder": bidder,
                "Amount": amount
            }
            print("\n-------------New Highest Bid-------------------")
            print("Item:", item["Name"])
            print("Bidder:", bidder)
            print("Amount: PHP", amount)
            print("-------------------------------------------------")
            self.send_bid_response(
                client_id,
                True,
                f"Your bid of PHP {amount} was accepted!"
            )
            self.publish_payload(bid_update, "auction/update")
    def send_bid_response(self, client_id,accepted, message):
        response ={
            "Accepted": accepted,
            "Message": message
        }
        response_topic = f"auction/bid/response/{client_id}"
        self.publish_payload(response, response_topic)
    def auction_timer(self):
        while self.active_item_id is not None:
            remaining = self.auction_deadline - time.monotonic()
            if remaining <= 0:
                self.admin_commands.put("timer_stop")
                break
            time.sleep(0.2)
if __name__ == "__main__":
    AuctionServer()
