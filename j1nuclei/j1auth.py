import j1nuclei.config


def get_auth_headers() -> dict:
    """
    Get JupiterOne authentication headers
    :return: headers including account and authorization token
    """
    auth_header = {
            "Content-Type": "application/json",
            "JupiterOne-Account": j1nuclei.config.j1_account,
            "Authorization": f"Bearer {j1nuclei.config.j1_api_key}"
            }

    return auth_header
