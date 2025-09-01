from typing import Union
from pymongo import MongoClient
from datetime import datetime
from fastapi import FastAPI, Query
from pydantic import BaseModel
from bson import ObjectId
import math

app = FastAPI()

# client = MongoClient("mongodb+srv://rentedmail:rentedmail@firstcluster.rifik5c.mongodb.net/")
client = MongoClient("mongodb://localhost:27017/")
db = client["digital_wallet"]
collection = db["users"]
transactions_collection = db["transactions"]


class Users(BaseModel):
    username: str
    email: str
    password: str
    phone_number: str
    balance: float

class Transactions(BaseModel):
    amount: float
    description: str

class Transfer(BaseModel):
    sender_user_id: str
    recipient_user_id: str
    amount: float
    description: str

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/users/{user_id}")
def get_users(user_id: str):
    id = ObjectId(user_id)
    user = collection.find_one({"_id": id})
    if user:
        return {"username": user["username"], "email": user["email"]}
    return {"error": "User not found"}


@app.post("/users/")
def create_user(user: Users):
    user_dict = user.dict()
    user_dict["created_at"] = datetime.now()
    user_dict["updated_at"] = datetime.now()
    collection.insert_one(user_dict)
    return {"username": user.username, "email": user.email}


@app.get("/wallet/{user_id}/balance")
def get_wallet_balance(user_id: str):
    id = ObjectId(user_id)
    user = collection.find_one({"_id": id})
    if user:
        return {
            "user_id": str(user["_id"]),
            "balance": user["balance"],
            "last_updated": user["updated_at"]
        }
    return {"error": "User not found"}


@app.post("/wallet/{user_id}/add-money")
def add_money_to_wallet(user_id: str, transaction: Transactions):
    id = ObjectId(user_id)
    user = collection.find_one({"_id": id})
    if user:
        # Update user balance
        new_balance = user["balance"] + transaction.amount
        collection.update_one({"_id": id}, {"$set": {"balance": new_balance}})

        # Create transaction record
        transaction_dict = transaction.dict()
        transaction_dict["user_id"] = user["_id"]
        transaction_dict["transaction_type"] = "CREDIT"
        transaction_dict["created_at"] = datetime.now()
        transaction_dict["reference_transaction_id"] = None
        transaction_dict["recipient_user_id"] =  None
        transactions_collection.insert_one(transaction_dict)

        return {
            "transaction_id": str(transaction_dict["_id"]),
            "user_id": str(transaction_dict["user_id"]),
            "amount": transaction.amount,
            "new_balance": new_balance,
            "transaction_type": "CREDIT"
        }
    return {"error": "User not found"}

@app.post("/wallet/{user_id}/withdraw")
def withdraw_from_wallet(user_id: str, transaction: Transactions):
    id = ObjectId(user_id)
    user = collection.find_one({"_id": id})
    if user:
        if user["balance"] >= transaction.amount:
            # Update user balance
            new_balance = user["balance"] - transaction.amount
            collection.update_one({"_id": id}, {"$set": {"balance": new_balance}})

            # Create transaction record
            transaction_dict = transaction.dict()
            transaction_dict["user_id"] = user["_id"]
            transaction_dict["transaction_type"] = "DEBIT"
            transactions_collection.insert_one(transaction_dict)

            return {
                "transaction_id": str(transaction_dict["_id"]),
                "user_id": str(transaction_dict["user_id"]),
                "amount": transaction.amount,
                "new_balance": new_balance,
                "transaction_type": "DEBIT"
            }
        return {"error": "Insufficient balance"}
    return {"error": "User not found"}


@app.get("/transactions/{user_id}")
def get_transactions(user_id: str, page: int = Query(1, ge=1), limit: int = Query(10, ge=1)):
    id = ObjectId(user_id)
    transactions = transactions_collection.find({"user_id": id})
    if transactions:
        total = transactions_collection.count_documents({"user_id": id})
        transactions = transactions_collection.find({"user_id": id}).skip((page - 1) * limit).limit(limit)
        list_of_transactions = []

        for tx in list(transactions):
            print(tx,"tx")
            tx["transaction_id"] = str(tx["_id"])
            tx["user_id"] = str(tx["user_id"])
            list_of_transactions.append(tx)

        print(list(list_of_transactions),"list_of_transactions")
        return {
            "page": page,
            "limit": limit,
        "total": total,
        "transactions": list_of_transactions
    }
    return {"error": "No transactions found"}


@app.get("/transactions/detail/{transaction_id}")
def get_transaction_detail(transaction_id: str):
    id = ObjectId(transaction_id)
    transaction = transactions_collection.find_one({"_id": id})
    if transaction:
        return {
            "transaction_id": str(transaction["_id"]),
            "user_id": str(transaction["user_id"]),
            "transaction_type": transaction["transaction_type"],
            "amount": transaction["amount"],
            "description": transaction["description"],
            "recipient_user_id": str(transaction["recipient_user_id"]),
            "reference_transaction_id": str(transaction["reference_transaction_id"]),
            "created_at": transaction["created_at"].isoformat()
        }
    return {"error": "Transaction not found"}

@app.post("/transfer")
def transfer_funds(transfer: Transfer):
    sender_id = ObjectId(transfer.sender_user_id)
    recipient_id = ObjectId(transfer.recipient_user_id)

    # Check sender's balance
    sender = collection.find_one({"_id": sender_id})
    if not sender:
        return {"error": "Sender not found"}, 404

    if sender["balance"] < transfer.amount:
        return {
            "error": "Insufficient balance",
            "current_balance": sender["balance"],
            "required_amount": transfer.amount
        }, 400

    # Process the transfer
    new_sender_balance = sender["balance"] - transfer.amount
    collection.update_one({"_id": sender_id}, {"$set": {"balance": new_sender_balance}})

    # Create transaction records
    sender_transaction = {
        "user_id": sender["_id"],
        "amount": transfer.amount,
        "transaction_type": "TRANSFER_OUT",
        "description": transfer.description,
        "recipient_user_id": recipient_id,
        "created_at": datetime.utcnow()
    }
    transactions_collection.insert_one(sender_transaction)

    # Transfer operations must create two linked transactions (debit + credit)
    recipient_transaction = {
        "user_id": recipient_id,
        "amount": transfer.amount,
        "transaction_type": "TRANSFER_IN",
        "description": transfer.description,
        "recipient_user_id": sender_id,
        "created_at": datetime.utcnow()
    }
    transactions_collection.insert_one(recipient_transaction)

    recipient = collection.find_one({"_id": recipient_id})
    if not recipient:
        return {"error": "Recipient not found"}, 404

    new_recipient_balance = recipient["balance"] + transfer.amount
    collection.update_one({"_id": recipient_id}, {"$set": {"balance": new_recipient_balance}})

    recipient_transaction = {
        "user_id": recipient["_id"],
        "amount": transfer.amount,
        "transaction_type": "TRANSFER_IN",
        "description": transfer.description,
        "recipient_user_id": sender_id,
        "created_at": datetime.utcnow()
    }
    transactions_collection.insert_one(recipient_transaction)

    return {
        "transfer_id": str(sender_transaction["_id"]),
        "sender_transaction_id": str(sender_transaction["_id"]),
        "recipient_transaction_id": str(recipient_transaction["_id"]),
        "amount": transfer.amount,
        "sender_new_balance": new_sender_balance,
        "recipient_new_balance": new_recipient_balance,
        "status": "completed"
    }, 201

    