import re
import string
from typing import List

import numpy as np

from cityhash import CityHash32

punctuation_pattern = re.compile(r'[{}]'.format(re.escape(string.punctuation)))


def hash_token(token: str, vector_size: int) -> int:
    """
    Hash a token into a vector index using a simple hash function.
    
    Parameters:
        token: str
            The token to hash.
        vector_size: int
            The size of the vector, to ensure the hash value falls within the vector's range.
    
    Returns:
        An integer representing the hashed vector index.
    """
    return CityHash32(token) % vector_size

def vectorize_with_hashing(tokens: List[str], vector_size: int = 1024) -> np.ndarray:
    """
    Vectorize a list of tokens using the hashing trick into a fixed-size vector.
    
    Parameters:
        tokens: List[str]
            A list of tokens to vectorize.
        vector_size: int, optional
            The size of the vector. Defaults to 1024.
    
    Returns:
        A numpy array of type uint16 representing the hashed BoW vector.
    """
    # Initialize a vector of zeros with uint16 type to simulate 2-byte storage
    vector = np.zeros(vector_size, dtype=np.uint16)
    for token in tokens:
        index = hash_token(token, vector_size)
        # Increment the count at the hashed index position
        # In a strict 2-byte environment, care must be taken to avoid overflow
        vector[index] += 1

        index_2 = int(index * 1.618033988749895) % vector_size
        vector[index_2] += 1
    return vector

def cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """
    Calculate the cosine similarity between two vectors.
    
    Parameters:
        vec1: np.ndarray
            The first vector.
        vec2: np.ndarray
            The second vector.
    
    Returns:
        The cosine similarity as a float.
    """
    # Ensure floating point division for similarity calculation
    vec1 = vec1.astype(np.float64)
    vec2 = vec2.astype(np.float64)
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

def tokenize(arr):
    # Replace each punctuation mark with a space
    no_punctuation = punctuation_pattern.sub(' ', arr)
    # Split the modified string into tokens by spaces and filter out empty tokens
    tokens = [token for token in no_punctuation.lower().split(' ') if token]
    return tokens

def similarity_engine_with_hashing(strings: List[str], comparison_string: str, vector_size: int = 1024) -> List[float]:
    """
    A simple vector similarity engine using hashing trick to compare a list of strings against another string.
    
    Parameters:
        strings: List[str]
            The list of strings to compare.
        comparison_string: str
            The string to compare against the list.
        vector_size: int, optional
            The size of the vectors, defaults to 1024.
    
    Returns:
        A list of similarity scores.
    """
    tokenized_strings = [tokenize(s) for s in strings] + [tokenize(comparison_string)]
    vectors = [vectorize_with_hashing(tokens, vector_size) for tokens in tokenized_strings]
    comparison_vector = vectors[-1]
    similarities = [cosine_similarity(vector, comparison_vector) for vector in vectors[:-1]]
    
    return similarities

# Example usage with "Pride and Prejudice" lines
strings = [
    "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife.",
    "However little known the feelings or views of such a man may be on his first entering a neighbourhood, this truth is so well fixed in the minds of the surrounding families, that he is considered as the rightful property of some one or other of their daughters.",
    "My dear Mr. Bennet, said his lady to him one day, have you heard that Netherfield Park is let at last?",
    "Mr. Bennet replied that he had not.",
    "But it is, returned she; for Mrs. Long has just been here, and she told me all about it.",
    "Mr. Bennet made no answer.",
    "Do not you want to know who has taken it? cried his wife impatiently.",
    "You want to tell me, and I have no objection to hearing it.",
    "This was invitation enough.",
    "Why, my dear, you must know, Mrs. Long says that Netherfield is taken by a young man of large fortune from the north of England; that he came down on Monday in a chaise and four to see the place, and was so much delighted with it, that he agreed with Mr. Morris immediately; that he is to take possession before Michaelmas, and some of his servants are to be in the house by the end of next week.",
    "What is his name?",
    "Bingley.",
    "Is he married or single?",
    "Oh! Single, my dear, to be sure! A single man of large fortune; four or five thousand a year. What a fine thing for our girls!",
    "How so? How can it affect them?",
    "My dear Mr. Bennet, replied his wife, how can you be so tiresome! You must know that I am thinking of his marrying one of them.",
    "Is that his design in settling here?",
    "Design! Nonsense, how can you talk so! But it is very likely that he may fall in love with one of them, and therefore you must visit him as soon as he comes.",
    "I see no occasion for that. You and the girls may go, or you may send them by themselves, which perhaps will be still better, for as you are as handsome as any of them, Mr. Bingley may like you the best of the party.",
    "My dear, you flatter me. I certainly have had my share of beauty, but I do not pretend to be anything extraordinary now. When a woman has five grown-up daughters, she ought to give over thinking of her own beauty.",
    "In such cases, a woman has not often much beauty to think of.",
    "But, my dear, you must indeed go and see Mr. Bingley when he comes into the neighbourhood.",
    "It is more than I engage for, I assure you.",
    "But consider your daughters. Only think what an establishment it would be for one of them. Sir William and Lady Lucas are determined to go, merely on that account, for in general, you know, they visit no newcomers. Indeed you must go, for it will be impossible for us to visit him if you do not.",
    "You are over-scrupulous, surely. I dare say Mr. Bingley will be very glad to see you; and I will send a few lines by you to assure him of my hearty consent to his marrying whichever he chooses of the girls; though I must throw in a good word for my little Lizzy.",
    "I desire you will do no such thing. Lizzy is not a bit better than the others; and I am sure she is not half so handsome as Jane, nor half so good-humoured as Lydia. But you are always giving her the preference."
    "They have none of them much to recommend them, replied he; they are all silly and ignorant like other girls; but Lizzy has something more of quickness than her sisters.",
    "Mr. Bennet, how can you abuse your own children in such a way? You take delight in vexing me. You have no compassion for my poor nerves.",
    "You mistake me, my dear. I have a high respect for your nerves. They are my old friends. I have heard you mention them with consideration these last twenty years at least.",
    "Ah, you do not know what I suffer.",
    "But I hope you will get over it, and live to see many young men of four thousand a year come into the neighbourhood.",
    "It will be no use to us, if twenty such should come, since you will not visit them.",
    "Depend upon it, my dear, that when there are twenty, I will visit them all.",
    "Mr. Bennet was so odd a mixture of quick parts, sarcastic humour, reserve, and caprice, that the experience of three-and-twenty years had been insufficient to make his wife understand his character.",
    "Her mind was less difficult to develop. She was a woman of mean understanding, little information, and uncertain temper.",
    "When she was discontented, she fancied herself nervous. The business of her life was to get her daughters married; its solace was visiting and news.",
    "Mr. Bennet was among the earliest of those who waited on Mr. Bingley. He had always intended to visit him, though to the last always assuring his wife that he should not go.",
    "Until the evening after the visit was paid she had no knowledge of it. It was then disclosed in the following manner.",
    "Observing his second daughter employed in trimming a hat, he suddenly addressed her with: I hope Mr. Bingley will like it, Lizzy.",
    "We are not in a way to know what Mr. Bingley likes, said her mother resentfully, since we are not to visit.",
    "But you forget, mamma, said Elizabeth, that we shall meet him at the assemblies, and that Mrs. Long promised to introduce him.",
    "I do not believe Mrs. Long will do any such thing. She has two nieces of her own. She is a selfish, hypocritical woman, and I have no opinion of her.",
    "No more have I, said Mr. Bennet; and I am glad to find that you do not depend on her serving you.",
    "Mrs. Bennet deigned not to make any reply, but, unable to contain herself, began scolding one of her daughters.",
    "Don't keep coughing so, Kitty, for Heaven's sake! Have a little compassion on my nerves. You tear them to pieces.",
    "Kitty has no discretion in her coughs, said her father; she times them ill.",
    "I do not cough for my own amusement, replied Kitty fretfully. When is your next ball to be, Lizzy?",
    "To-morrow fortnight.",
    "Aye, so it is, cried her mother, and Mrs. Long does not come back till the day before; so it will be impossible for her to introduce him, for she will not know him herself.",
    "Then, my dear, you may have the advantage of your friend, and introduce Mr. Bingley to her.",
    "Impossible, Mr. Bennet, impossible, when I am not acquainted with him myself; how can you be so teasing?"
    "2024-04-07T10:15:00Z INFO User 'admin' logged in from 192.168.1.5",
    "2024-04-07T10:17:32Z WARNING Attempted login for 'admin' from 192.168.1.6 failed: Incorrect password",
    "2024-04-07T10:20:10Z INFO User 'jdoe' logged in from 192.168.1.7",
    "2024-04-07T10:22:45Z ALERT Possible SQL injection attack detected from 192.168.1.8",
    "2024-04-07T10:25:00Z INFO User 'admin' logged out",
    "2024-04-07T10:30:00Z INFO Backup process started by user 'backup_service'",
    "2024-04-07T10:35:15Z WARNING Attempted login for 'root' from 192.168.1.9 failed: Incorrect password",
    "2024-04-07T10:40:05Z INFO User 'jdoe' accessed resource '/confidential_data'",
    "2024-04-07T10:45:22Z ALERT Data exfiltration detected: Large data transfer from 192.168.1.10 to external address",
    "2024-04-07T10:50:30Z INFO User 'jdoe' logged out",
    "2024-04-07T10:55:00Z INFO Scheduled system scan started",
    "2024-04-07T11:00:00Z ALERT Firewall breach detected from 192.168.1.11",
    "2024-04-07T11:05:00Z INFO System update started by user 'update_service'",
    "2024-04-07T11:10:00Z WARNING Disk space on device 'server01' is below 10%",
    "2024-04-07T11:15:00Z INFO System update completed successfully"
]


comparison_strings = [
    "With the Gardiners, they were always on the most intimate terms. Darcy, as well as Elizabeth, really loved them;"
    "They were both ever sensible of the warmest gratitude towards the persons who, by bringing her into Derbyshire, had been the means of uniting them.",
    "And so Darcy did what he could to forget the scorn which he had before felt for his attachment, as he affectionately termed it, and gradually did away all the old misunderstandings between them.",
    "Their sister's wedding day arrived; and Jane and Elizabeth felt for her probably more than she felt for herself."
    "The carriage was sent to meet them at ---, and they were to return in it by dinner-time.",
    "Their arrival was dreaded by the elder Miss Bennets, and Jane more especially, who gave Lydia the feelings which would have attended herself"
    "Had she been the culprit, and was wretched in the thought of what her sister must endure.",
    "They were welcomed with great cordiality everywhere; as they had been always esteemed a credit to their family by the neighbourhood"
    "and when they parted from each other, it was generally understood that it had been a temporary separation."
]

comparison_strings = [
    "2024-04-07T11:20:00Z WARNING Attempted login for 'admin' from 192.168.1.12 failed: Incorrect password"
]

for compare in comparison_strings:
    similarities = similarity_engine_with_hashing(strings, compare)
    #print(similarities)
    index_of_best_match = similarities.index(max(similarities))
    print(f"{compare}\n({similarities[index_of_best_match]}) {strings[index_of_best_match]}\n")

firewall_1 = CityHash32("attempted") % 1024
firewall_2 = int(firewall_1 * 1.618033988749895) % 1024

hits = 0
for s in strings:
    vec = vectorize_with_hashing(s.lower().split(" "))
    if vec[firewall_1] != 0 and vec[firewall_2] != 0:
        hits += 1
        print(s)
print(hits)
