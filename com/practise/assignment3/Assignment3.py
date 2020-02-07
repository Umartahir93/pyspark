from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import socket

CONSUMER_KEY = '0gFJpitshfdNVG3tO8b9tJQtv'
CONSUMER_SECRET = 'gebAwM8gKTZ9ydXIfaeoHOdRbSxmAU5PpcqAuCStXRfKH2XseL'
ACCESS_TOKEN = '145640128-wDBnlhLdyXVhBKmFdPnao7cwCS6QMK9cRlxQzltI'
ACCESS_TOKEN_SECRET = 'F26FzI2gTXFqoSSrAVoYrLh6awDWVD6tXo7fDwPXM5A95'


class StdOutListener(StreamListener):
    def __init__(self, clt):
        self.client_socket = clt

    def on_data(self, data):
        try:
            print(data)
            self.client_socket.send(data.encode('utf-8'))
            return True

        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


class Twitter():
    def stream_tweets(self, hash_tag_list,clt):
        listener = StdOutListener(clt)
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)  # https://github.com/psf/requests/issues/3883
        stream = Stream(auth, listener)
        stream.filter(track=hash_tag_list)


if __name__ == '__main__':
    hash_tag_list = ["#PakistanNeedsPMLN", "#coronavirusoutbreak"]
    twitter = Twitter()
    socket_s = socket.socket()
    host = "localhost"
    port = 5555
    socket_s.bind((host, port))
    print("Listening on port: %s" % str(port))
    socket_s.listen(20)
    clt,addr = socket_s.accept()

    twitter.stream_tweets(hash_tag_list,clt)



