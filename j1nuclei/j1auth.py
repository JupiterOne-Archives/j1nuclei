import j1nuclei.config


def get_auth_headers() -> dict:
    auth_header = {
            "Content-Type": "application/json",
            "JupiterOne-Account": j1nuclei.config.j1_account,
            "Authorization": "Bearer {}".format(j1nuclei.config.j1_api_key)
            }

    return auth_header
