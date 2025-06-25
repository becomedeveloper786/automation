import random
from typing import List, Dict, Any

def split_contacts(contacts: List[Dict[str, Any]], primary_percent: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    Shuffles and splits a list of contacts into two campaigns based on a percentage.

    Args:
        contacts: A list of contact dictionaries.
        primary_percent: The percentage of contacts to allocate to the primary campaign.

    Returns:
        A dictionary with two keys, 'campaign_a' and 'campaign_b', containing the split lists.
    """
    if not 0 <= primary_percent <= 100:
        raise ValueError("Percentage must be between 0 and 100.")

    random.shuffle(contacts)
    
    split_index = int(len(contacts) * (primary_percent / 100))
    
    return {
        'campaign_a': contacts[:split_index],
        'campaign_b': contacts[split_index:]
    }