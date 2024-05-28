import requests


def send_simple_message(to,subject,body):
    url = 'https://api.mailgun.net/v3/{}/messages'.format('mg.flynava.com')
    auth = ('api', 'key-c013b61ff3e42ee652acb378576a97b8')
    data = {
        'from': 'Jupiter<jupiter@{}>'.format('mg.flynava.com'),
        'to': to,
        'subject': subject,
        'html':  body,
    }
    response = requests.post(url, auth=auth, data=data)


if __name__ == '__main__':
    send_simple_message("abhinav.garg@flynava.com","test","hi there")