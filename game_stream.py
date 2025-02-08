import requests
import json

def stream_lichess_game(game_id):
    """
    Connects to Lichess API and streams game events for the specified game using the 
    'https://lichess.org/api/stream/game/{id}' endpoint.
    
    :param game_id: The Lichess game ID.
    """
    url = f"https://lichess.org/api/stream/game/{game_id}"
    headers = {"Accept": "application/x-ndjson"}

    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        print("Failed to connect, status code:", response.status_code)
        return

    print("Connected to Lichess game stream!")
    print("Waiting for game events...")

    # Process each line (each line is a JSON object)
    for line in response.iter_lines(decode_unicode=True):
        if not line:
            continue  # Skip empty lines

        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            print("Could not decode line:", line)
            continue

        # The streamed event might include a "type" field to indicate the kind of event.
        event_type = event.get("type")
        if event_type == "gameFull":
            # The initial event when connecting contains full game details.
            print("Game started!")
            moves_str = event.get("moves", "")
            if moves_str:
                move_list = moves_str.split()
                print("Initial moves:", move_list)
        elif event_type == "gameState":
            # Subsequent updates to the game state.
            moves_str = event.get("moves", "")
            if moves_str:
                move_list = moves_str.split()
                print("Moves so far:", move_list)
        else:
            # Sometimes the endpoint may stream other events or a final game result.
            print("Received event:", event)

if __name__ == "__main__":
    # Replace 'your_game_id_here' with the actual game ID.
    game_id = "jVX55QcKxUZu"
    # If needed (for private games), set your Lichess API token here; otherwise, leave as None.
    stream_lichess_game(game_id)