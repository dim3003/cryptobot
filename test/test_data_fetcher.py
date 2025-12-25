import re
from datafetcher.main import get_available_tokens

ETH_ADDRESS_REGEX = re.compile(r"^0x[a-fA-F0-9]{40}$")

def test_get_available_tokens_format():
    tokens = get_available_tokens()

    # basic structure
    assert isinstance(tokens, list)
    assert len(tokens) > 0

    # format validation
    for token in tokens:
        assert isinstance(token, str)
        assert ETH_ADDRESS_REGEX.match(token)

    # no duplicates
    assert len(tokens) == len(set(tokens))

