import paramiko

username = 'root'

password = 'Pycf@12#$'

hostname = '192.168.0.153'

port = 56567

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname, port, username, password)

stdin, stdout, stderr = ssh.exec_command("who")

print(stdout.readlines())

ssh.close()
