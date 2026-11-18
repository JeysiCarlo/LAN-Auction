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
        self.clients = {}
        self.auction_invitation_responses = {}
        self.auction_bidders = set()
        self.active_invitation_item_id = None
        self.invitation_duration = 10
        self.invitation_deadline = None
        self.invitation_thread = None
        self.next_item_id = 1
        self.active_item_id = None
        self.auction_duration = 30
        self.auction_deadline = None
        self.timer_thread = None
        self.timer_version = 0
        super().__init__(
            process_name = "Auction Server",
            receive_loop_idle_addition = self.process_admin_commands
        )
        self.set_subscriber_topic("auction/bid")
        self.set_subscriber_topic("auction/item/submit")
        self.set_subscriber_topic("auction/client/join")
        self.set_subscriber_topic("auction/client/leave")
        self.set_subscriber_topic("auction/invitation/response")
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
        print("exit")
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
            parts = command.split()
            if not parts:
                continue
            action = parts[0].lower()
            if action != "timer_update":
                processed_any = True
            if action == "timer_update":
                if len(parts) != 3:
                    continue
                if int(parts[1]) != self.timer_version:
                    continue
                if self.active_item_id is None:
                    continue
                timer_update ={
                    "Remaining": int(parts[2]),
                    "ItemID": self.active_item_id
                }
                self.publish_payload(timer_update, "auction/timer")
                continue
            if action == "invitation_finish":
                if len(parts) != 2:
                    continue
                item_id = int(parts[1])
                if self.active_invitation_item_id != item_id:
                    continue
                item = self.items[item_id]
                for client_id, client in self.clients.items():
                    if client_id == item["SellerClientID"]:
                        continue
                    if client_id not in self.auction_invitation_responses:
                        client["State"] = "spectator"
                self.active_invitation_item_id = None
                self.invitation_deadline = None
                item["Status"] = "active"
                item["CurrentPrice"] = item["StartingPrice"]
                item["HighestBidder"] = None
                item["BidHistory"] = []
                self.active_item_id = item_id
                self.auction_deadline = time.monotonic() + self.auction_duration
                self.timer_version += 1
                current_timer_version = self.timer_version
                self.timer_thread = threading.Thread(
                    target = self.auction_timer,
                    args = (current_timer_version,),
                    daemon = True
                )
                self.timer_thread.start()
                self.publish_payload(item, "auction/item/update")
                print("\nAuction Started!")
                print("Item:", item["Name"])
                print("Auction duration:", self.auction_duration, "seconds")
                continue
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
                    if (
                        self.active_item_id is not None
                        or self.active_invitation_item_id is not None
                    ):
                        print("Another auction or invitation is already active.")
                        continue
                    self.active_invitation_item_id = item_id
                    self.invitation_deadline = (
                        time.monotonic() + self.invitation_duration
                    )
                    self.send_auction_invitations(item, item_id)
                    print("\n Waiting for invitation responses....")
                    print(
                        "Invitation duration:",
                        self.invitation_duration,
                        "seconds"
                    )
                    self.invitation_thread = threading.Thread(
                        target = self.invitation_timer,
                        args = (item_id,),
                        daemon = True
                    )
                    self.invitation_thread.start()
            elif action in ["stop", "timer_stop"]:
                if action == "timer_stop":
                    if len(parts) < 2:
                        continue
                    if int(parts[1]) != self.timer_version:
                        continue
                    if self.auction_deadline is None:
                        continue
                    if time.monotonic() < self.auction_deadline:
                        continue
                    print("\nAuction Timer Finished!")
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
                self.auction_deadline = None
                self.timer_version += 1
                self.auction_bidders.clear()
                self.auction_invitation_responses.clear()
                for client in self.clients.values():
                    client["State"] = "lobby"
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
            elif action in ["exit", "shutdown", "quit"]:
                force = len(parts) == 2 and parts[1].lower() == "force"
                auction_busy = (
                    self.active_item_id is not None
                    or self.active_invitation_item_id is not None
                )
                if auction_busy and not force:
                    print("An auction or invitation is still in progress.")
                    print("Type 'stop' to end it first, or 'exit force' to quit anyway.")
                    continue
                self.shutdown_server()
            elif action == "help":
                print("\nAvailable commands:")
                print("list")
                print("approve <ID>")
                print("reject <ID>")
                print("start <ID>")
                print("stop")
                print("History")
                print("History <ID>")
                print("exit")
                print("exit force")
            else:
                print("Unknown command. Type help")
        if processed_any:
            self.command_processed.set()
    def shutdown_server(self):
        print("\nShutting down server...")
        self.active_invitation_item_id = None
        self.invitation_deadline = None
        self.active_item_id = None
        self.auction_deadline = None
        self.timer_version += 1
        self.publish_payload(
        {"Message": "The auction server is shutting down."},
        "auction/server/shutdown")
        time.sleep(0.5)
        raise KeyboardInterrupt
    def incoming_message_processing(self, topic, payload):
        with self.console_lock:
            print("\n--Incoming message received--")
            print("Topic:", topic)
            print("Payload:", repr(payload))
            print("Payload type:", type(payload).__name__)
        if topic == "auction/invitation/response":
            if not isinstance(payload, dict):
                return
            client_id = payload.get("ClientID")
            item_id = payload.get("ItemID")
            response = payload.get("Response")
            if self.active_invitation_item_id is None:
                return
            if (
                self.invitation_deadline is None
                or time.monotonic() >= self.invitation_deadline
            ):
                return
            if item_id != self.active_invitation_item_id:
                return
            if client_id not in self.clients:
                return
            if response not in ["accept", "reject"]:
                return
            if client_id in self.auction_invitation_responses:
                return
            item = self.items[self.active_invitation_item_id]
            if client_id == item["SellerClientID"]:
                return
            self.auction_invitation_responses[client_id] = response
            if response == "accept":
                self.auction_bidders.add(client_id)
                self.clients[client_id]["State"] = "bidder"
                print(f"\n[Invitation] {self.clients[client_id]['Name']} accepted.")
            else:
                self.auction_bidders.discard(client_id)
                self.clients[client_id]["State"] = "spectator"
                print(f"\n[Invitation] {self.clients[client_id]['Name']} rejected.")
        if topic == "auction/client/join":
            if not isinstance(payload, dict):
                return
            client_id = payload.get("ClientID")
            name = payload.get("Name")
            if not client_id or not name:
                return
            if (
                self.active_item_id is not None
                or self.active_invitation_item_id is not None
            ):
                state = "spectator"
            else: 
                state = "lobby"
            self.clients[client_id] = {
                "ClientID": client_id,
                "Name": name,
                "State": state
            }
            print(f"[Lobby] {name}'s state: {self.clients[client_id]['State']}")
            print(f"\n[Lobby] {name} joined the lobby.")
            announcement = {
                "Message": f"{name} joined the lobby."
            }
            self.publish_payload(
                announcement,
                "auction/lobby/announcement"
            )
            lobby_update = {
                "Clients": list(self.clients.values())
            }
            self.publish_payload(lobby_update, "auction/lobby/update")
        elif topic == "auction/client/leave":
            if not isinstance(payload, dict):
                return
            client_id = payload.get("ClientID")
            if client_id in self.clients:
                name = self.clients[client_id]["Name"]
                self.auction_bidders.discard(client_id)
                self.auction_invitation_responses.pop(client_id, None)
                del self.clients[client_id]
                print(f"\n[Lobby] {name} left the lobby.")
                announcement = {
                    "Message": f"{name} left the lobby."
                }
                self.publish_payload(
                    announcement,
                    "auction/lobby/announcement"
                )
                lobby_update = {
                    "Clients": list (self.clients.values())
                }
                self.publish_payload(lobby_update, "auction/lobby/update")
        if topic == "auction/item/submit":
            if not isinstance(payload, dict):
                print("Invalid item submission")
                return
            required_fields = [
                "Seller",
                "Name",
                "Description",
                "StartingPrice",
                "SellerClientID"
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
                "Status": "pending",
                "SellerClientID": payload["SellerClientID"]
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
            if client_id not in self.auction_bidders:
                message = "You are not an accepted bidder for this auction."
                print("Bid rejected:", message)
                self.send_bid_response(client_id,False,message)
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
            self.auction_deadline = time.monotonic() + self.auction_duration
            self.timer_version += 1
            current_timer_version = self.timer_version
            self.timer_thread = threading.Thread(
                target = self.auction_timer,
                args = (current_timer_version,),
                daemon = True
            )
            self.timer_thread.start()
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
    def send_bid_response(self, client_id, accepted, message):
        response = {
            "Accepted": accepted,
            "Message": message
        }
        topic = f"auction/bid/response/{client_id}"
        self.publish_payload(response, topic)
    def auction_timer(self, timer_version):
        last_remaining = None
        while (
            self.active_item_id is not None
            and timer_version == self.timer_version
        ):
            if self.auction_deadline is None:
                break
            remaining = self.auction_deadline - time.monotonic()
            if remaining <= 0:
                self.admin_commands.put(f"timer_stop {timer_version}")
                break
            remaining_seconds = int(remaining + 0.999999)
            if remaining_seconds != last_remaining:
                self.admin_commands.put(
                    f"timer_update {timer_version} {remaining_seconds}" 
                )
                last_remaining = remaining_seconds
            time.sleep(0.2)
    def invitation_timer(self, item_id):
        while self.active_invitation_item_id == item_id:
            remaining = self.invitation_deadline - time.monotonic()
            if remaining <= 0:
                self.admin_commands.put(
                    f"invitation_finish {item_id}"
                )
                break
            self.publish_payload(
                {"ItemID": item_id, "Remaining": int(remaining)},
                "auction/invitation/timer"
            )
            time.sleep(1)
    def send_auction_invitations(self, item, item_id):
        self.auction_invitation_responses.clear()
        self.auction_bidders.clear()
        invitation = {
            "ItemID": item_id,
            "Name": item["Name"],
            "Seller": item["Seller"],
            "StartingPrice": item["StartingPrice"],
            "SellerClientID": item["SellerClientID"]
        }       
        self.publish_payload(
            invitation,
            "auction/invitation"
        )
        print("\nInvitations sent to lobby clients.")
if __name__ == "__main__":
    AuctionServer()
