
def get_hash_tags(tweet_text_data):
    return tweet_text_data["entities"]["hashtags"]

def get_user_mentions(tweet_text_data):
    return tweet_text_data["entities"]["user_mentions"]
