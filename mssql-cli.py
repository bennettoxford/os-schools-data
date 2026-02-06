import os

import env

os.environ["SQLCMDPASSWORD"] = env.password

cmd = ["sqlcmd", "-S", env.server, "-U", env.user, "-d", env.database]

os.execvp(cmd[0], cmd)
