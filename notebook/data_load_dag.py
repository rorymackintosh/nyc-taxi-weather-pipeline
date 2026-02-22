from pymongo import MongoClient
import pandas as pd
from google.cloud import storage

client = MongoClient("mongodb://localhost:27017/")