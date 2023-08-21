from kiteconnect import KiteConnect

if __name__ == '__main__':

    # autologin()
    # access_token = open("access_token.txt", 'r).read()
    key_secret = open("api_key.txt", 'r').read().split()
    kite = KiteConnect(api_key=key_secret[0])
    print(kite.login_url())

    data = kite.generate_session(
        "tNsgQDOfNgkTHvtlrq6ptkQzvu4Uv7on", api_secret=key_secret[1])
    kite.set_access_token(data["access_token"])
    print(data["access_token"])
    # print(kite.orders())
