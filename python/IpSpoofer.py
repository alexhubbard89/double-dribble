import pandas as pd
import requests
from BeautifulSoup import BeautifulSoup
import os
from fake_useragent import UserAgent

class IpSpoofer(object):
    """
    This will be used to find a random IP and Port to proxy to.
    The need for this is because congress Blacklists a website when
    it looks like a bot, and doesn't allow blacklisted IPs to access
    their website. So to get aroudn this I am using a proxy IP address.
    """
    @staticmethod
    def request_page(url, as_json=False):
        """
        Because there are problems requesting pages measure have to be
        put in place to get around the limitations. Rather than writing
        redundant code this function will be used to handle url requests.
        """

        counter = 0
        status_good = False
        ua = UserAgent()
        print url

        while status_good == False:
            ip_df = IpSpoofer.random_ip()
            ## Get prxoy IP address
            ip = str(ip_df.loc[0, 'ip'])
            port = str(ip_df.loc[0, 'port'])
            s = requests.session()
            proxies = {
              'http': '{}:{}'.format(ip, port),
            }
            s.proxies.update(proxies)
            a = requests.adapters.HTTPAdapter(max_retries=5)
            if 'https://' in url:
                s.mount('https://', a)
            else:
                s.mount('http://', a)
            s.headers.update({'User-Agent': ua.random})
            r = s.get(url)

            print r.status_code
            if r.status_code == 200:
                if "400 Bad Request" not in str(r.content):
                    status_good = True
                    if as_json == True:
                        return r.json()
                    else:
                        return BeautifulSoup(r.content)
            elif r.status_code == 404:
                return 'Status: 404'

            counter +=1
            if counter > 10:
                break

    @staticmethod
    def random_ip():
        """
        User service to find random IP and port
        Args:
            None
        Returns:
            Dataframe with ip address and port
        """
        r = requests.get('https://api.getproxylist.com/proxy?apiKey={}&country[]=US'.format(os.environ['PROXY_LIST']))
        ip_df = pd.DataFrame([[r.json()['ip'], r.json()['port']]], columns=['ip', 'port']).reset_index(drop=True)
        return ip_df
