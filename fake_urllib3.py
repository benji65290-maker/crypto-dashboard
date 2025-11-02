# fake_urllib3.py – compatibilité pour python-telegram-bot 13.x sous Python 3.13
import sys
import types

# Simule le package urllib3.contrib.appengine
fake_contrib = types.ModuleType("urllib3.contrib.appengine")
sys.modules["urllib3.contrib.appengine"] = fake_contrib

# Simule le package telegram.vendor.ptb_urllib3.urllib3.packages.six.moves
fake_moves = types.ModuleType("telegram.vendor.ptb_urllib3.urllib3.packages.six.moves")
sys.modules["telegram.vendor.ptb_urllib3.urllib3.packages.six.moves"] = fake_moves
