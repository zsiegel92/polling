import requests

api_key = os.environ.get("API_KEY")   # GOOGLE API - Auyon/Nic


def get_current_elections():
    url = f'https://www.googleapis.com/civicinfo/v2/elections?key={api_key}'
    response = requests.get(url).json()
    return response['elections']


def visualize(elections):
    for election in elections:
        print(f"{election['name']} (ID: {election['id']}), on {election['electionDay']}")

if __name__=="__main__":
    elections = get_current_elections()
    visualize(elections)


