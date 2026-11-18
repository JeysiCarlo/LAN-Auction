from python_banyan.banyan_base import BanyanBase
from prompt_toolkit import prompt, print_formatted_text
from prompt_toolkit.patch_stdout import patch_stdout
import sys
import queue
import threading
import uuid
import ipaddress
import socket
class AuctionClient(BanyanBase):
    def __init__(self, name, ip_address):
        self.outgoing_queue = queue.Queue()
        self.role = "seller"
        self.name = name
        self.console_lock = threading.Lock()
        self.message_processed = threading.Event()
        self.shutdown_event = threading.Event()
        self.client_id = uuid.uuid4().hex
        self.current_invitation = None
        self.invitation_status = None
        self.auction_active = False
        self.last_auction_timer_remaining = None
        self.bid_response_topic = f"auction/bid/response/{self.client_id}"
        self.history_response_topic = f"auction/history/response/{self.client_id}"
        self.invitation_remaining = None
        self.auction_remaining = None
        super().__init__(
            back_plane_ip_address = ip_address,
            process_name = "Auction Client",
            receive_loop_idle_addition = self.process_pending_messages
        )
        self.set_subscriber_topic("auction/update")
        self.set_subscriber_topic("auction/item/update")
        self.set_subscriber_topic(self.bid_response_topic)
        self.set_subscriber_topic(self.history_response_topic)
        self.set_subscriber_topic("auction/timer")
        self.set_subscriber_topic("auction/lobby/update")
        self.set_subscriber_topic("auction/invitation")
        self.set_subscriber_topic("auction/invitation/timer")
        self.set_subscriber_topic("auction/lobby/announcement")
        self.set_subscriber_topic("auction/server/shutdown")
        join_payload = {
            "ClientID": self.client_id,
            "Name": self.name
        }
        self.publish_payload(join_payload, "auction/client/join")
        print("\nWelcome to the LAN Auction System")
        print("Role: Seller (Lobby)")
        print("Name:", self.name)
        print("Press Ctrl + C to exit.\n")
        input_thread = threading.Thread(
            target = self.main_input_loop,
            daemon = True
        )
        try:
            with patch_stdout():
                input_thread.start()
                self.receive_loop()
        except KeyboardInterrupt:
            print("\nShutting down client...")
            sys.exit(0)
    def submit_item(self):
        print("\n------Submit an item-------")
        item_name = prompt("Item name: ").strip()
        while not item_name:
            print("Item name cannot be empty.")
            item_name = prompt("Item name: ").strip()
        description = prompt("Item description: ").strip()
        while not description:
            print("Description cannot be empty.")
            description = prompt("Item description: ").strip()
        while True:
            try:
                starting_price = float(
                    prompt("Starting price(PHP): ")
                )
                if starting_price <= 0:
                    print("Must not be zero or less than zero.")
                    continue
                break
            except ValueError:
                print("Invalid price. Please enter a number.")
        item = {
            "Seller": self.name,
            "Name": item_name,
            "Description": description,
            "StartingPrice": starting_price,
            "SellerClientID": self.client_id
        }
        self.outgoing_queue.put(
            ("auction/item/submit", item)
        )
        self.message_processed.wait()
        self.message_processed.clear()
    def get_item_details(self):
        try:
            while True:
                self.submit_item()
        except KeyboardInterrupt:
            print("\nClient shutting off...")
            self.shutdown_event.set()
    def bottom_toolbar(self):
        parts = []
        if self.invitation_remaining is not None:
            parts.append(f"Invitation: {self.invitation_remaining}s left")
        if self.auction_remaining is not None:
            parts.append(f"Auction: {self.auction_remaining}s left")
        return " | ".join(parts) if parts else "No active timers"
    def main_input_loop(self):
        try:
            while True:
                print("\n--------------LAN AUCTION--------------")
                print("Commands:")
                print("submit - Submit an item")
                print("accept - Accept an auction invitation")
                print("reject - Reject an auction invitation")
                print("bid - place a bid")
                print("history - view completed auctions")
                print("history <ID> - view details and bid history of an auction")
                print("leave - Exit the auction system")
                print("help - Show available commands")
                print("-----------------------------------------")
                command = prompt("Command: ", bottom_toolbar = self.bottom_toolbar,refresh_interval = 0.5).strip().lower()
                parts = command.split()
                if command == "submit":
                    self.submit_item()
                elif command == "accept":
                    if self.current_invitation is None:
                        print("No pending auction invitation.")
                        continue
                    response = {
                        "ClientID": self.client_id,
                        "ItemID": self.current_invitation["ItemID"],
                        "Response": "accept"
                    }
                    self.outgoing_queue.put(
                        ("auction/invitation/response", response)
                    )
                    self.invitation_status = "accept"
                    self.current_invitation = None
                    self.invitation_remaining = None 
                    print("You accepted the Invitation.")
                elif command == "reject":
                    if self.current_invitation is None:
                        print("No pending auction invitation.")
                        continue
                    response = {
                        "ClientID": self.client_id,
                        "ItemID": self.current_invitation["ItemID"],
                        "Response": "reject"
                    }
                    self.outgoing_queue.put(
                        ("auction/invitation/response", response)
                    )
                    self.invitation_status = "reject"
                    self.current_invitation = None
                    self.invitation_remaining = None 
                    print("You reject the invitation. You can till view the bid")
                elif command == "bid":
                    if self.invitation_status != "accept":
                        print("You are not an accepted bidder")
                        continue
                    if not self.auction_active:
                        print("There's no active auction.")
                        continue
                    try:
                        amount = float(prompt("Your bid(PHP): "))
                        if amount <= 0:
                            print("Bid amount must not be zero or less than it.")
                            continue
                        bid_payload = {
                            "ClientID": self.client_id,
                            "Bidder": self.name,
                            "Amount": amount
                        }
                        self.outgoing_queue.put(
                            ("auction/bid", bid_payload)
                        )
                    except ValueError:
                        print("Invalid amount. Please enter a valid number.")
                elif parts and parts[0] == "history":
                    if len(parts) == 1:
                        request = {"ClientID": self.client_id}
                    elif len(parts) == 2 and parts[1].isdigit():
                        request = {
                            "ClientID": self.client_id,
                            "AuctionID": int(parts[1])
                        }
                    else:
                        print("Usage: history or history <ID>")
                        continue
                    self.outgoing_queue.put(
                        ("auction/history/request", request)
                        )
                elif command == "help":
                    print("\n---------------HELP----------------")
                    print("submit - Submit an item")
                    print("accept - Accept an auction invitation")
                    print("reject - Reject an auction invitation")
                    print("bid    - Place a bid")
                    print("history - View completed auctions")
                    print("history <ID> - View details and bid history of an auction")
                    print("leave  - Exit the auction system")
                    print("help   - Show available commands")
                    print("-------------------------------------")
                elif command =="leave":
                    self.shutdown_event.set()
                    break
                else:
                    print("Invalid Command.")
        except KeyboardInterrupt:
            print("\nClient shutting off....")
            self.shutdown_event.set()
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
            print("\nCLient shutting off...")
            self.shutdown_event.set()
    def process_pending_messages(self):
        if self.shutdown_event.is_set():
            leave_paylaod = {
                "ClientID": self.client_id
            }
            self.publish_payload(leave_paylaod,"auction/client/leave")
            raise KeyboardInterrupt
        while not self.outgoing_queue.empty():
            topic, payload = self.outgoing_queue.get()
            self.publish_payload(payload, topic)
            if topic == "auction/item/submit":
                    print("\nItem submission sent to the server.")
            self.message_processed.set()
    def incoming_message_processing(self, topic, payload):
        if topic == "auction/lobby/update":
            if not isinstance(payload, dict):
                return
            clients = payload.get("Clients",[])
            print("\n---------Lobby--------")
            if not clients:
                print("No clients present in the lobby.")
            else:
                for client in clients:
                    print(f"-{client['Name']}")
            print("------------------------")
        if topic == "auction/lobby/announcement":
            if not isinstance(payload, dict):
                return
            message = payload.get("Message")
            if message:
                print(f"\n[Lobby]{message}")
        if topic == "auction/invitation":
            if not isinstance(payload, dict):
                return
            if payload.get("SellerClientID") == self.client_id:
                return
            self.current_invitation = payload
            self.invitation_status = None
            print("\n-------------Auction Invitation--------------")
            print("Item:", payload.get("Name", "Unknown"))
            print("Seller:", payload.get("Seller", "Unknown"))
            print("Starting Price(PHP):", payload.get("StartingPrice", "Unknown"))
            print("Accept or reject invitation.")
            print("-----------------------------------------------")
        if topic == "auction/invitation/timer":
            if not isinstance(payload, dict):
                return
            if self.current_invitation is None:
                return
            if payload.get("ItemID") != self.current_invitation.get("ItemID"):
                return
            self.invitation_remaining = payload.get("Remaining")
        if topic == "auction/item/update":
            if not isinstance(payload, dict):
                return
            self.auction_active = payload.get("Status") == "active"
            self.invitation_remaining = None              
            if payload.get("Status") != "active":         
                self.auction_remaining = None             
            if payload.get("Status") == "completed":
                self.invitation_status = None
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
        elif topic == self.history_response_topic:
            if not isinstance(payload, dict):
                return
            if not payload.get("Success"):
                print("\n[History]", payload.get("Message", "Request failed."))
                return
            if payload.get("Type") == "list":
                auctions = payload.get("Auctions", [])
                print("\n---------------Auction History-----------------")
                if not auctions:
                    print("No completed auctions yet.")
                for a in auctions:
                    print("\nAuction ID:", a["ID"])
                    print("Item:", a["Name"])
                    print("Seller:", a["Seller"])
                    print("Starting Price: PHP", a["StartingPrice"])
                    print("Final Price: PHP", a["FinalPrice"])
                    print("Winner:", a["Winner"] or "No bids")
                print("---------------------------------------------------")
                print("Type 'history <ID>' for bid details.")
            elif payload.get("Type") == "detail":
                a = payload["Auction"]
                print("\n========== AUCTION DETAILS ==========")
                print("Auction ID:", a["ID"])
                print("Item:", a["Name"])
                print("Seller:", a["Seller"])
                print("Starting Price: PHP", a["StartingPrice"])
                print("Final Price: PHP", a["FinalPrice"])
                print("Winner:", a["Winner"] or "No bids")
                print("\n--------------------- Bid History-------------------")
                if not a["BidHistory"]:
                    print("No bids were placed.")
                else:
                    for number, bid in enumerate(a["BidHistory"], start=1):
                        print(number, "| Bidder:", bid["Bidder"],
                            "| Amount: PHP", bid["Amount"])
                print("------------------------------------------------------")
        elif topic == "auction/timer":
            if not isinstance(payload, dict):
                return
            remaining = payload.get("Remaining")
            self.auction_remaining = remaining if remaining else None
            self.last_auction_timer_remaining = remaining
        elif topic == "auction/server/shutdown":
            if not isinstance(payload, dict):
                return
            print("\n[Server]", payload.get("Message", "Server is shutting down."))
            print("Disconnecting...")
            self.shutdown_event.set()
def backplane_reachable(ip_address, timeout = 3):
    for port in (43124, 43125):
        try:
            with socket.create_connection((ip_address, port), timeout=timeout):
                pass
        except OSError:
            return False
    return True
def get_user_details():
    print("-------------------------------------")
    print("            LAN AUCTION              ")
    print("-------------------------------------")
    while True:
        ip_address = input("Enter the server IP address: ").strip()
        try:
            ipaddress.ip_address(ip_address)
        except ValueError:
            print("Invalid IP address. Example: 192.168.1.10")
            continue
        print("Checking connection...")
        if backplane_reachable(ip_address):
            break
        print("Cannot reach the backplane at that address.")
        print("Check the IP, make sure the backplane is running, and that both devices are on the same network.")
    name = input("Enter your display name: ").strip()
    while not name:
        print("Name cannot be empty.")
        name = input("Enter your display name: ").strip()
    return ip_address, name
if __name__ == "__main__":
    ip_address, name = get_user_details()
    AuctionClient(name, ip_address)