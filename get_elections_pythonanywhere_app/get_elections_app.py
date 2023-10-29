from flask import Flask
from dotenv import load_dotenv
import os
import requests
load_dotenv()

api_key = os.environ.get("API_KEY")    # GOOGLE API - Auyon/Nic

app = Flask(__name__)


def get_current_elections():
	api_key = os.getenv("api_key")
    url = f'https://www.googleapis.com/civicinfo/v2/elections?key={api_key}'
    response = requests.get(url).json()
    return response['elections']


def visualize(elections):
    for election in elections:
        print(f"{election['name']} (ID: {election['id']}), on {election['electionDay']}")


@app.route("/")
def election_results():
	elections = get_current_elections()
	visualize(elections)
	return elections


