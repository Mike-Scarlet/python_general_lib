
from .misc import ParsePastedKVPair

general_header_str = """
                     Connection: keep-alive
                     Upgrade-Insecure-Requests: 1
                     User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36
                     Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
                     Accept-Encoding: gzip, deflate, br
                     Accept-Language: en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7
                     """
			 
general_header = ParsePastedKVPair(general_header_str)