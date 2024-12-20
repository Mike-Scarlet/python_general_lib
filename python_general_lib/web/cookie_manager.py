import pickle, os
import requests, logging
import atexit

class CookieManager:
  def __init__(self, cookie_path=None):
    self.logger = logging.getLogger("CookieManager")
    self.cookie_storage_path = cookie_path
    if not os.path.exists(self.cookie_storage_path):
      self.cookie_jar = None
    else:
      with open(self.cookie_storage_path, 'rb') as f:
        self.cookie_jar = pickle.load(f)

  def Init(self, session):
    if self.cookie_jar is None:
      self.cookie_jar = session.cookies
    else:
      session.cookies.update(self.cookie_jar)
      self.cookie_jar = session.cookies

  def Has(self, key):
    return key in self.cookie_jar.get_dict()

  def _exit(self):
    if self.cookie_jar != None:
      with open(self.cookie_storage_path, 'wb') as f:
        pickle.dump(self.cookie_jar, f)
      self.logger.info("cookie saved")

  def SetCookieSavePath(self, path):
    self.cookie_storage_path = path

  def RegisterAutoCookieSave(self):
    if self.cookie_storage_path is None:
      self.logger.error("cannot RegisterAutoCookieSave when cookie_storage_path is None")
      return
    atexit.register(self._exit)