import re

# Load stopwords once (global for simplicity)
def load_stopwords(filepath='stopwords.txt'):
    with open(filepath, 'r', encoding='utf-8') as f:
        stopwords = set(word.strip().lower() for word in f if word.strip())
    return stopwords

STOPWORDS = load_stopwords()

# Preprocess a single text string
def preprocess(text:str):
    """
    Tokenizes, lowercases, removes stopwords and one-letter tokens from input text.

    Args:
        text (str): Raw review text

    Returns:
        List[str]: Cleaned list of tokens
    """
    # Tokenize using the given set of delimiters
    tokens = re.split(r'[\s\d()\[\]{}.!?,;:+=\-_"\'`~#@&*%€$§\\/]+', text)

    # Lowercase and filter
    cleaned_tokens = [
        token.lower() for token in tokens
        if token and len(token) > 1 and token.lower() not in STOPWORDS
    ]

    return cleaned_tokens

# Quick test
if __name__ == '__main__':
    sample_text = """
    This was a gift for my other husband. He's making us things from it all the time!
    Directions are simple, easy to read and interpret.
    """
    print(preprocess(sample_text))
