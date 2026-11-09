from flask import Flask
from threading import Thread
import os

# This creates a global Flask app instance
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running on Render!"

def run():
    # Render dynamically assigns a port
    port = int(os.environ.get("PORT", 8080))
    # Run the web server
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    """
    Starts the Flask web server in a separate thread so it doesn't block your Discord bot.
    """
    server_thread = Thread(target=run)
    server_thread.start()