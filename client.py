from python_banyan.banyan_base import BanyanBase
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout
import sys
import queue
import threading
import uuid
class AuctionClient(BanyanBase):
    def __init__(self, role, name):
        self.outgoing_queue = queue.Queue()
        self.role = role
        self.name = name
        self.console_lock = threading.Lock()
        self.message_processed = threading.Event()
        self.client_id = uuid.uuid4().hex
        self.bid_response_topic = f"auction/bid/response/{self.client_id}"
        super().__init__(
            process_name = "Auction Client",
            receive_loop_idle_addition = self.process_pending_messages
        )
        self.set_subscriber_topic("auction/update")
        self.set_subscriber_topic("auction/item/update")
        self.set_subscriber_topic(self.bid_response_topic)
        print("\nWelcome to the LAN Auction System")
        print("Role:", self.role.capitalize())
        print("Name:", self.name)
        print("Press Ctrl + C to exit.\n")
        if self.role == "seller":
            input_thread = threading.Thread(
                target = self.get_item_details,
                daemon = True
            )
        else:
            input_thread = threading.Thread(
                target = self.get_bid_amounts,
                daemon = True
            )
        try:
            with patch_stdout():
                input_thread.start()
                self.receive_loop()
        except KeyboardInterrupt:
            print("\nShutting down client...")
            sys.exit(0)
    def get_item_details(self):
        while True:
            print("\n------Submit an Item------")
            item_name = prompt("Item name: ").strip()
            while not item_name:
                print("Item name cannot be empty.")
                item_name = prompt("Item name: ").strip()
            description = prompt("item description: ").strip()
            while not description:
                print("Description cannot be empty.")
                description = prompt("Item description: ").strip()
            try:
                starting_price = float(
                    prompt("Starting price(PHP): ")
                )
                if starting_price <= 0:
                    print("Must not be zero or less than zero")
                    continue
            except ValueError:
                print("Invalid price. Please enter a number.")
                continue
            item = {
                "Seller": self.name,
                "Name": item_name,
                "Description": description,
                "StartingPrice": starting_price
            }
            self.outgoing_queue.put(
                ("auction/item/submit", item)
            )
            self.message_processed.wait()
            self.message_processed.clear()
    def get_bid_amounts(self):
        try:
            while True:
                try:
                    amount = float(
                        prompt("Enter your bid amount (PHP): ")
                    )
                    if amount <= 0:
                        print("Bid amount must not be zero or less")
                        continue
                    bid = {
                        "ClientID": self.client_id,
                        "Bidder": self.name,
                        "Amount": amount
                    }
                    self.outgoing_queue.put(
                        ("auction/bid", bid)
                    )
                except ValueError:
                    print("Invalid input. Please enter a valid number.")
        except KeyboardInterrupt:
            print("\nBid input stopped.")

    def process_pending_messages(self):
        while not self.outgoing_queue.empty():
            topic, payload = self.outgoing_queue.get()
            self.publish_payload(payload, topic)
            if topic == "auction/item/submit":
                    print("\nItem submission sent to the server.")
            self.message_processed.set()
    def incoming_message_processing(self, topic, payload):
        if topic == "auction/item/update":
            if not isinstance(payload, dict):
                return
            print("\n------Auction Item Update-------")
            print("Item:", payload.get("Name", "Unknown"))
            print("Seller:", payload.get("Seller", "Unknown"))
            print("Starting Price(PHP);",
                payload.get("StartingPrice", "Unknown"))
            print("Status", payload.get("Status", "Unknown"))
            if payload.get("Status") == "active":
                print("Current Price(PHP):",
                    payload.get("CurrentPrice","Unknown"))
            elif payload.get("Status") == "completed":
                print("Auction has ended.")
                print("Winner:",
                    payload.get("HighestBidder") or "No bids")
                print("Final Price (PHP):",
                    payload.get("CurrentPrice","Unknown"))
            print("----------------------------------------")
        elif topic == "auction/update":
            if not isinstance(payload, dict):
                    return
            print("\n---New Highest Bid---")
            print("Item:", payload.get("ItemName","Unknown"))
            print("Bidder:", payload.get("Bidder", "Unknown"))
            print("Amount (PHP):", payload.get("Amount", "Unknown"))
            print("------------------------------------------")
        elif topic == self.bid_response_topic:
            if not isinstance(payload, dict):
                return
            message = payload.get("Message", "No response message.")
            if payload.get("Accepted"):
                print("\n[BID ACCEPTED]", message)
            else:
                print("\n[BID REJECTED]", message)
def get_user_details():
    print("-------------------------------------")
    print("            LAN AUCTION              ")
    print("-------------------------------------")
    print("1. Seller")
    print("2. Bidder")
    while True:
        choice = input("Select your role: ").strip()
        if choice == "1":
            role = "seller"
            break
        elif choice == "2":
            role ="bidder"
            break
        else:
            print("Invalid Selection. Choose either 1 or 2.")
    name = input("Enter your name: ").strip()
    while not name:
        print("Name cannot be empty.")
        name = input("Enter your name: ").strip()
    return role, name
if __name__ == "__main__":
    role, name = get_user_details()
    AuctionClient(role, name)