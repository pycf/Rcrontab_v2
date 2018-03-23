from email.header import Header
from email.utils import parseaddr, formataddr
import queue
from threading import Thread
from time import sleep
queue_mail = queue.Queue(100)


class SendMail:
    def __init__(self):
        pass

    @staticmethod
    def _format_addr(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))

    def _send_mail(self,  msg, to_addr, to=None, subject=None, ):
        if subject:
            subject = subject
        else:
            subject = '未定义'
        if to:
            to = to
        else:
            to = ""
        print(msg, to_addr, to, subject)
        # msg = MIMEText(msg, 'plain', 'utf-8')
        # from_addr = 'huruizhi@pystandard.com'
        # password = '510543763@hrz'
        # smtp_server = 'smtp.mxhichina.com'
        # to_addr = to_addr
        # msg['From'] = self._format_addr('普益IT - 程序调度管理平台 <%s>' % from_addr)
        # msg['To'] = self._format_addr("{to} <{to_addr}>".format(to=to, to_addr=to_addr))
        # msg['Subject'] = Header(subject, 'utf-8').encode()
        # server = smtplib.SMTP(smtp_server, 25)
        # # SMTP协议默认端口是25
        # server.set_debuglevel(1)
        # server.login(from_addr, password)
        # server.sendmail(from_addr, [to_addr, ], msg.as_string())
        # server.quit()

    def __call__(self, *args, **kwargs):
        while True:
            mail_info = queue_mail.get()
            args, kwargs = mail_info
            print(args, kwargs)
            try:
                self._send_mail(*args, **kwargs)
            except Exception as e:
                print("SendMail Err:"+str(e))
            finally:
                sleep(10)


def create_mail(msg, to_addr, to=None, subject=None):
        args = (msg, to_addr)
        kwargs = {'to': to, 'subject': subject}
        queue_mail.put((args, kwargs))


if __name__ == '__main__':
    t = Thread(target=SendMail())
    t.start()
    message = 'Hello , send by Python...'
    to_address = '510543763@qq.com'
    create_mail(message, to_address, to='hurz', subject='运维邮件测试')
