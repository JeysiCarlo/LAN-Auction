from python_banyan.banyan_base import BanyanBase
import sys
import queue
import threading

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
                    print("Starting Price(PHP):", item["Starting Price"])
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
                    self.active_item_id = item_id
                    self.publish_payload(item, "auction/item/update")
                    print("\nAuction Started!")
                    print("Item:", item["Name"])
                elif action == "stop":
                    if self.active_item_id is None:
                        print("There is no active auction")
                        continue
                    item = self.items[self.active_item_id]
                    item["Status"] = "completed"
                    self.publish_payload(item, "auction/item/update")
                    print("Auction Ended:", item["Name"])
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
                "Starting Price"
            ]
            if not all(field in payload for field in required_fields):
                print("Invalid item submission")
                return
            try:
                starting_price = float(payload["Starting Price"])
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
            if isinstance(payload, dict):
                print("Payload keys:", [repr(key) for key in payload.keys()])
            if isinstance(payload, dict) and "Bidder" in payload and "Amount" in payload:
                print("New bid received!")
                print("Bidder:", payload["Bidder"])
                print("Amount:", payload["Amount"])
                self.publish_payload(payload, "auction/update")
            else:
                print("Invalid bid format")
if __name__ == "__main__":
    AuctionServer()
